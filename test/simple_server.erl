-module(simple_server).

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
  { ok, {} }.

handle_call( { delay_task, DelayMS, N }, _From, State ) ->
  timer:sleep( DelayMS ),
  { reply, N, State };
handle_call( { echo, Msg }, _From, State ) ->
  { reply, Msg, State };
handle_call( { die_after, DieAfterMS }, _From, State ) ->
  erlang:send_after( DieAfterMS, self(), time_to_die ),
  { reply, ok, State }.

handle_cast( { send_message, To, Msg }, State ) ->
  To ! Msg,
  { noreply, State };
handle_cast( Msg, State ) ->
  error_logger:info_msg( "~s:~B - ~s:handle_cast/2 - unknown message, Msg:[~p]~n",
                         [ ?FILE, ?LINE, ?MODULE, Msg ] ),
  { noreply, State }.

handle_info( time_to_die, State ) ->
  { stop, normal, State };
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
