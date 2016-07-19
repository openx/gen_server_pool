%%%-------------------------------------------------------------------
%%% File    : gen_server_pool_test.erl
%%% Author  : Vikram Kadi <vikram.kadi@openx.org>
%%% Description :
%%%   Tests for gen_server_pool
%%%
%%% Created :  24 Feb 2015
%%%-------------------------------------------------------------------
-module(gen_server_pool_test).

-behaviour(gen_server).

-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3 ]).

-export([ start_link/1 ]).


start_link([]) ->
  gen_server:start_link( ?MODULE, [], [] ).

init( [] ) ->
  process_flag( trap_exit, true ),
  {ok, {}}.

handle_call( { delay_task, MS, N }, _From, State ) ->
  timer:sleep( MS ),
  { reply, N, State };
handle_call( { echo, Msg }, _From, State ) ->
  { reply, Msg, State }.

handle_cast( { send_message, To, Msg }, State ) ->
  To ! Msg,
  { noreply, State };
handle_cast( Msg, State ) ->
  error_logger:info_msg("~s:~B - ~s:handle_cast/2 - unknown message, Msg:[~p]~n",
                [?FILE,?LINE,?MODULE,Msg]),
  { noreply, State }.

handle_info( _Info, State ) ->
  { noreply, State }.

terminate( Reason, _State ) ->
  case Reason of
    shutdown -> ok;
    _ ->
      error_logger:info_msg( "~s:~B - ~s:terminate/2 - reason ~w\n",
                             [ ?FILE, ?LINE, ?MODULE, Reason ] ),
      ok
  end.

code_change( _OldVsn, State, _Extra ) ->
  { ok, State }.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

max_worker_wait_test () ->
  PoolId = testpool,
  MaxQueue = 4,
  MaxPoolSize = 20,
  PoolOpts = [ { min_pool_size, 10 },
               { max_pool_size, MaxPoolSize },
               { idle_timeout, 60 },
               { request_max_wait, 50 },
               { max_queue, MaxQueue },
               { prog_id, testpool },
               { pool_id, PoolId } ],
  gen_server_pool:start_link(
    { local, PoolId },
    gen_server_pool_test, [], [], PoolOpts),

  %% Test basic functionality.
  Msg1 = <<"test one">>,
  ?assertEqual( Msg1, gen_server:call( PoolId, { echo, Msg1 } ) ),

  %% Test cast.
  MsgCast = <<"test cast">>,
  ?assertEqual( ok, gen_server:cast( PoolId, { send_message, self(), MsgCast } ) ),
  CastResponse =
  receive
    CastMsg -> CastMsg
  after
    100 -> timeout
  end,
  ?assertEqual( MsgCast, CastResponse ),

  SpawnTasks =
    fun ( Count ) ->
        lists:foreach(
          fun ( N ) -> spawn( fun () ->
                                  case gen_server:call( PoolId, { delay_task, 200, N } ) of
                                    N -> %% io:format( standard_error, "got ~p\n", [ N ] ),
                                         ok;
                                    R -> io:format( standard_error, "unexpected response ~p for ~p\n", [ R, N ] )
                                  end
                              end ),
                       timer:sleep( 1 )
          end, lists:seq( 1, Count ) )
    end,

  %% Test request_timeout.
  %% This request will time out.  It will be queued, but by the time it is
  %% removed from the queue request_max_wait will have been exceeded.
  SpawnTasks( MaxPoolSize ),
  Result = gen_server:call( PoolId, { delay_task, 100, 99 } ),
  ?assertEqual( { error, request_timeout }, Result ),

  %% Test request_dropped.  Spawn one request.  This will sit in the
  %% queue. Then spawn enough more to fill up the queue.  The first request
  %% will be dropped.
  SpawnTasks( MaxPoolSize ),
  Parent = self(),
  Msg2 = <<"test two">>,
  Pid2 = spawn( fun () -> Parent ! { self(), gen_server:call( PoolId, { echo, Msg2 } ) } end ),
  timer:sleep( 1 ),
  SpawnTasks( MaxQueue ),

  Result2 =
    receive
      { Pid2, Result2A } -> Result2A
    end,
  %% timer:sleep( 200 ),
  ?assertEqual( { error, request_dropped }, Result2 ),

  timer:sleep( 200 ),

  %% This request should succeed.
  Msg3 = <<"test three">>,
  ?assertEqual( Msg3, gen_server:call( PoolId, { echo, Msg3 } ) ),

  gen_server:cast( PoolId, stop ),

  ok.

-endif. %% TEST
