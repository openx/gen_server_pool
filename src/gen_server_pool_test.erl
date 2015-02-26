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

handle_call( {delay_task, Millis}, _From, State) ->
  timer:sleep(Millis),
  { reply, ok, State }.

handle_cast( Msg, State ) ->
  error_logger:info_msg("~s:~B - ~s:handle_cast/2 - unknown message, Msg:[~p]~n",
                [?FILE,?LINE,?MODULE,Msg]),
  { noreply, State }.

handle_info( _Info, State ) ->
  { noreply, State }.

terminate( Reason, _State ) ->
  case Reason of
    normal   -> ok;
    shutdown -> ok;
    _Else     ->
      error_logger:info_msg("~s:~B - ~s:terminate/2 - reason ~w~n",
                    [?FILE,?LINE,?MODULE,Reason]),
      ok
  end.

code_change( _OldVsn, State, _Extra ) ->
  { ok, State }.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

max_worker_wait_test_ () ->
  PoolId = testpool,
  MaxPoolSize = 20,
  PoolOpts = [ { min_pool_size, 2 },
               { max_pool_size, MaxPoolSize },
               { idle_timeout, 60 },
               { request_max_wait, 50 },
               { max_queue, 20 },
               { prog_id, testpool },
               { pool_id, PoolId } ],
  gen_server_pool:start_link(
    { local, PoolId },
    gen_server_pool_test, [], [], PoolOpts),
  %% Fire off requests to get all the workers busy
  [ spawn(fun() -> gen_server:call(PoolId, {delay_task, 200}) end)
      || _ <- lists:seq(1, MaxPoolSize) ],
  timer:sleep(5),
  %% This is the request that should timeout as the wait will exceed
  %% request_max_wait
  Result=gen_server:call(PoolId, {delay_task, 100}),
  ?_assertEqual({error, request_timeout}, Result).

-endif.
