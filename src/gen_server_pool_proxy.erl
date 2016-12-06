%%%-------------------------------------------------------------------
%%% File    : gen_server_pool_proxy.erl
%%% Author  : Joel Meyer <joel@openx.org>
%%% Description : 
%%%   Serves as a proxy to gen_server.
%%%
%%% Created :  5 May 2011
%%%-------------------------------------------------------------------
-module(gen_server_pool_proxy).

-behaviour(gen_server).

-include("gen_server_pool_internal.hrl").

%% API
-export([ start_link/6,
          stop/2 ]).

%% gen_server callbacks
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3 ]).

-record( state, { manager_pid,
                  proxy_ref,
                  module,
                  noreply_target,
                  worker_start_time,
                  worker_end_time,
                  state
                } ).

%%====================================================================
%% API
%%====================================================================
start_link( MgrPid, ProxyRef, MaxWorkerAgeMS, Module, Args, Options ) ->
  gen_server:start_link( ?MODULE, [ MgrPid, ProxyRef, MaxWorkerAgeMS, Module, Args ], Options ).

stop( Pid, ProxyRef ) ->
  gen_server:cast( Pid, { ProxyRef, stop } ).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init( [ MgrPid, ProxyRef, MaxWorkerAgeMS, Module, Args ] ) ->
  Now = os:timestamp(),
  random:seed(Now),

  PState = #state{ manager_pid = MgrPid,
                   proxy_ref   = ProxyRef,
                   module      = Module,
                   worker_start_time = Now,
                   worker_end_time = worker_end_time( MaxWorkerAgeMS, Now ) },

  case Module:init( Args ) of
    { ok, State } ->
      gen_server_pool:available( MgrPid, ProxyRef, self(), PState#state.worker_end_time, ?GSP_FLAG_INITIAL ),
      { ok, state( PState, State ) };
    { ok, State, Extra } ->
      gen_server_pool:available( MgrPid, ProxyRef, self(), PState#state.worker_end_time, ?GSP_FLAG_INITIAL ),
      { ok, state( PState, State ), Extra };
    Other ->
      Other
  end.


handle_call( { ProxyRef, stop },
             _From,
             #state{ proxy_ref = ProxyRef } = PState ) ->
  { stop, normal, ok, PState };

handle_call( Msg,
             {_, FromRef}  = From,
             #state{ manager_pid = MgrPid,
                     proxy_ref   = ProxyRef,
                     module      = M,
                     worker_end_time = WorkerEndTime,
                     state       = S } = PState ) ->
  % we are switching out the pid in the From field with the pid of this
  % proxy, since if an embedded gen_server is using gen_server:reply/2
  % and thus using the noreply forms, we don't want to make the resource
  % available.
  case M:handle_call( Msg, { self(), FromRef }, S ) of
    { reply, Reply, NewS } ->
      gen_server_pool:available( MgrPid, ProxyRef, self(), WorkerEndTime, ?GSP_FLAG_NONE ),
      { reply, Reply, state( PState, NewS ) };
    { reply, Reply, NewS, Extra } ->
      gen_server_pool:available( MgrPid, ProxyRef, self(), WorkerEndTime, ?GSP_FLAG_NONE ),
      { reply, Reply, state( PState, NewS ), Extra };
    { noreply, NewS } ->
      { noreply, state( PState#state{ noreply_target = From }, NewS ) };
    { noreply, NewS, Extra } ->
      { noreply, state( PState#state{ noreply_target = From }, NewS ), Extra };
    { stop, Reason, Reply, NewS } ->
      { stop, Reason, Reply, state( PState, NewS ) };
    { stop, Reason, NewS } ->
      { stop, Reason, state( PState, NewS ) }
  end.


handle_cast( { ProxyRef, stop },
             #state{ proxy_ref = ProxyRef } = PState ) ->
  { stop, normal, PState };

handle_cast( Msg,
             #state{ manager_pid = MgrPid,
                     proxy_ref   = ProxyRef,
                     module      = M,
                     worker_end_time = WorkerEndTime,
                     state       = S } = PState ) ->
  case M:handle_cast( Msg, S ) of
    { noreply, NewS } ->
      gen_server_pool:available( MgrPid, ProxyRef, self(), WorkerEndTime, ?GSP_FLAG_NONE ),
      { noreply, state( PState, NewS ) };
    { noreply, NewS, Extra } ->
      gen_server_pool:available( MgrPid, ProxyRef, self(), WorkerEndTime, ?GSP_FLAG_NONE ),
      { noreply, state( PState, NewS ), Extra };
    { stop, Reason, NewS } ->
      { stop, Reason, state( PState, NewS ) }
  end.


% handle_info normally doesn't make resources available as it is used for
% things like gen_tcp/udp and other internal messaging.
%
% trap proxied noreply responses and send them back to the noreply_target.
handle_info( {Tag, ProxyMsg},
             #state{ manager_pid = MgrPid,
                     proxy_ref = ProxyRef,
                     worker_end_time = WorkerEndTime,
                     noreply_target = {Target,Tag}
                   } = PState ) ->
  Target ! { Tag, ProxyMsg },
  gen_server_pool:available( MgrPid, ProxyRef, self(), WorkerEndTime, ?GSP_FLAG_NONE ),
  { noreply, PState#state{ noreply_target = undefined } };
handle_info( Msg,
             #state{ module      = M,
                     state       = S } = PState ) ->
  case M:handle_info( Msg, S ) of
    { noreply, NewS } ->
      { noreply, state( PState, NewS ) };
    { noreply, NewS, Extra } ->
      { noreply, state( PState, NewS ), Extra };
    { stop, Reason, NewS } ->
      { stop, Reason, state( PState, NewS ) }
  end.


terminate( Reason,
           #state{ module = M,
                   state  = S } ) ->
  M:terminate( Reason, S ).


code_change( OldVsn,
             #state{ module = M, state = S } = PState,
             Extra ) ->
  { ok, NewS } = M:code_change( OldVsn, S, Extra ),
  {ok, state( PState, NewS ) }.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
state( ProxyState, State ) ->
  ProxyState#state{ state = State }.

worker_end_time( MaxWorkerAgeMS, Now ) when is_integer( MaxWorkerAgeMS ), MaxWorkerAgeMS > 0 ->
  gen_server_pool:now_add( Now, MaxWorkerAgeMS * 1000 );
worker_end_time( { MaxWorkerAgeMS, JitterMS }, Now ) when is_integer(MaxWorkerAgeMS), is_integer(JitterMS) ->
  worker_end_time( MaxWorkerAgeMS + random:uniform( JitterMS ), Now );
worker_end_time( { Module, Function, Args }, Now ) when is_atom( Module ), is_atom( Function ), is_list( Args ) ->
  worker_end_time( apply( Module, Function, Args ), Now );
worker_end_time( infinity, _Now ) ->
  infinity.


%%--------------------------------------------------------------------
%%% Tests
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
