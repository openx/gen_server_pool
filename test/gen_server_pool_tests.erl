-module(gen_server_pool_tests).

-include_lib("eunit/include/eunit.hrl").

%%-define(OUTPUT_STREAM, user).
-define(OUTPUT_STREAM, standard_io).

-define(BASIC_MINPOOL, 2).
-define(BASIC_MAXPOOL, 4).
-define(BASIC_MAXQUEUE, 4).
-define(BASIC_MAXWAIT, 50).

basic_setup() ->
  PoolId = basic_pool,
  PoolOpts = [ { min_pool_size, 2 },
               { max_pool_size, ?BASIC_MAXPOOL },
               { max_queued_requests, ?BASIC_MAXQUEUE },
               { max_request_wait_ms, ?BASIC_MAXWAIT } ],
  gen_server_pool:start_link( { local, PoolId }, simple_server, [], [], PoolOpts),
  PoolId.

cleanup( PoolId ) ->
  gen_server_pool:stop( PoolId ).

basic_test_() ->
  { "Basic Tests",
    inorder,
    { setup, fun basic_setup/0, fun cleanup/1,
      { with, [ fun t_stats/1
              , fun t_call/1
              , fun t_cast/1
              , fun t_stats/1
              ] } } }.

t_stats( PoolId ) ->
  Stats = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( ?BASIC_MINPOOL, proplists:get_value( size, Stats ) ),
  ?assertEqual( 0, proplists:get_value( active, Stats ) ),
  ?assertEqual( 0, proplists:get_value( tasks, Stats ) ).

t_call( PoolId ) ->
  %% Test basic functionality.
  Msg1 = <<"test one">>,
  ?assertEqual( Msg1, gen_server:call( PoolId, { echo, Msg1 } ) ).

t_cast( PoolId ) ->
  %% Test cast.
  MsgCast = <<"test cast">>,
  ?assertEqual( ok, gen_server:cast( PoolId, { send_message, self(), MsgCast } ) ),
  CastResponse =
  receive
    CastMsg -> CastMsg
  after
    100 -> timeout
  end,
  ?assertEqual( MsgCast, CastResponse ).


-define(TIMEOUT_POOL, timeout_pool).
-define(TIMEOUT_MINPOOL, 2).
-define(TIMEOUT_MAXPOOL, 4).
-define(TIMEOUT_MAXWAIT, 40).
-define(TIMEOUT_MAXQUEUE, 2).

timeout_setup() ->
  PoolId = ?TIMEOUT_POOL,
  PoolOpts = [ { min_pool_size, ?TIMEOUT_MINPOOL },
               { max_pool_size, ?TIMEOUT_MAXPOOL },
               { max_queued_requests, ?TIMEOUT_MAXQUEUE },
               { max_request_wait_ms, ?TIMEOUT_MAXWAIT } ],
  gen_server_pool:start_link( { local, PoolId }, simple_server, [], [], PoolOpts),
  PoolId.

spawn_tasks( PoolId, First, Count, Delay ) ->
  lists:foreach(
    fun ( N ) ->
        spawn( fun () ->
                   case gen_server:call( PoolId, { delay_task, Delay, N } ) of
                     N -> %% io:format( ?OUTPUT_STREAM, "got ~p\n", [ N ] ),
                          ok;
                     R -> io:format( ?OUTPUT_STREAM, "unexpected response ~p for ~p\n", [ R, N ] )
                   end
               end ),
        timer:sleep( 1 )
    end, lists:seq( First, First + Count - 1 ) ).

timeout_test_() ->
  { "Timeout Tests",
    inorder,
    { foreach, fun timeout_setup/0, fun cleanup/1,
      %% This syntax "works" but setup/cleanup is not called for each test:
      %% [ { with, [ fun t_timeout/1
      %%           , fun t_dropped/1
      %%           , fun t_call/1
      %%           ] } ]
      [ ?_test( t_timeout( ?TIMEOUT_POOL ) )
      , ?_test( t_dropped( ?TIMEOUT_POOL ) )
      , ?_test( t_call( ?TIMEOUT_POOL ) )
      , ?_test( t_monitor( ?TIMEOUT_POOL ) )
      ]
    } }.

t_timeout( PoolId ) ->
  Stats1 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( ?TIMEOUT_MINPOOL, proplists:get_value( size, Stats1 ) ),
  ?assertEqual( 0, proplists:get_value( drops, Stats1 ) ),

  %% Test request_timeout.
  %% This request will time out.  It will be queued, but by the time it is
  %% removed from the queue max_request_wait_ms will have been exceeded.
  spawn_tasks( PoolId, 1, ?TIMEOUT_MAXPOOL, ?TIMEOUT_MAXWAIT * 2 ),
  Result = gen_server:call( PoolId, { delay_task, 100, 99 } ),
  ?assertEqual( { error, request_timeout }, Result ),

  Stats2 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( ?TIMEOUT_MAXPOOL, proplists:get_value( size, Stats2 ) ),
  ?assertEqual( 1, proplists:get_value( drops, Stats2 ) ),

  timer:sleep(10).

t_dropped( PoolId ) ->
  Stats1 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( ?TIMEOUT_MINPOOL, proplists:get_value( size, Stats1 ) ),
  ?assertEqual( 0, proplists:get_value( drops, Stats1 ) ),

  %% Test request_dropped.  Spawn one request.  This will sit in the
  %% queue. Then spawn enough more to fill up the queue.  The first request
  %% will be dropped.
  spawn_tasks( PoolId, 1, ?TIMEOUT_MAXPOOL, ?TIMEOUT_MAXWAIT ),
  Parent = self(),
  Msg = <<"test two">>,
  Pid = spawn( fun () -> Parent ! { self(), gen_server:call( PoolId, { echo, Msg } ) } end ),
  timer:sleep( 1 ),
  spawn_tasks( PoolId, ?TIMEOUT_MAXPOOL, ?TIMEOUT_MAXPOOL, 1 ),

  Result =
    receive
      { Pid, R } -> R
    end,
  ?assertEqual( { error, request_dropped }, Result ),

  Stats2 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( ?TIMEOUT_MAXPOOL, proplists:get_value( size, Stats2 ) ),
  ?assert(proplists:get_value( drops, Stats2 ) >= 1 ).

t_monitor( PoolId ) ->
  DieDelay = 200,
  Stats1 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( ?TIMEOUT_MINPOOL, proplists:get_value( size, Stats1 ) ),
  ?assertEqual( ?TIMEOUT_MINPOOL, proplists:get_value( size_monitor, Stats1 ) ),
  spawn_tasks( PoolId, 1, ?TIMEOUT_MAXPOOL, ?TIMEOUT_MAXWAIT ),
  gen_server:call( PoolId, { die_after, DieDelay } ),
  Stats2 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( ?TIMEOUT_MAXPOOL, proplists:get_value( size, Stats2 ) ),
  ?assertEqual( ?TIMEOUT_MAXPOOL, proplists:get_value( size_monitor, Stats2 ) ),
  timer:sleep( DieDelay + 10 ),
  Stats3 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( ?TIMEOUT_MAXPOOL - 1, proplists:get_value( size, Stats3 ) ),
  ?assertEqual( ?TIMEOUT_MAXPOOL - 1, proplists:get_value( size_monitor, Stats3 ) ).


-define(AGE_AGE, 100).
age_setup() ->
  PoolId = age_pool,
  PoolOpts = [ { min_pool_size, 0 },
               { max_pool_size, 1 },
               { max_worker_age_ms, ?AGE_AGE } ],
  gen_server_pool:start_link( { local, PoolId }, simple_server, [], [], PoolOpts),
  PoolId.

age_test_() ->
  { "Age Tests",
    inorder,
    { setup, fun age_setup/0, fun cleanup/1,
      { with, [ fun t_age/1
              ] } } }.

t_age( PoolId ) ->
  Stats1 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( 0, proplists:get_value( size, Stats1 ) ),
  Pid1 = gen_server:call( PoolId, { worker_pid, 0 } ),
  Stats2 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( 1, proplists:get_value( size, Stats2 ) ),
  timer:sleep( ?AGE_AGE div 2 ),
  ?assertEqual( Pid1, gen_server:call( PoolId, { worker_pid, ?AGE_AGE + 10 } ) ),
  %% There should be no living workers because the previous call made the
  %% (one) worker outlive its lifetime, and so it should have been killed
  %% rather than be returned to the available pool.
  Stats3 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( 0, proplists:get_value( size, Stats3 ) ),
  %% And so a new worker will be started for this request.
  Pid2 = gen_server:call( PoolId, { worker_pid, 10 } ),
  ?assertNotEqual( Pid1, Pid2 ),
  Stats4 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( 1, proplists:get_value( size, Stats4 ) ),
  timer:sleep( ?AGE_AGE ),
  %% The previous worker, if it is still alive, has outlived its lifetime and
  %% so will be killed and a new worker started for the next request.
  Pid3 = gen_server:call( PoolId, { worker_pid, 10 } ),
  ?assertNotEqual( Pid2, Pid3 ),
  Stats5 = gen_server_pool:get_stats( PoolId ),
  ?assertEqual( 1, proplists:get_value( size, Stats5 ) ).
