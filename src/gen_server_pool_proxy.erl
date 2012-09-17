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

%% API
-export([ start_link/5,
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
                  state
                } ).

%%====================================================================
%% API
%%====================================================================
start_link( MgrPid, ProxyRef, Module, Args, Options ) ->
  gen_server:start_link( ?MODULE,
                         [ MgrPid, ProxyRef, Module, Args ], Options ).

stop( Pid, ProxyRef ) ->
  gen_server:call( Pid, { ProxyRef, stop } ).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init( [ MgrPid, ProxyRef, Module, Args ] ) ->
  PState = #state{ manager_pid = MgrPid,
                   proxy_ref   = ProxyRef,
                   module      = Module },

  case Module:init( Args ) of
    { ok, State } ->
      gen_server_pool:available( MgrPid, ProxyRef, self() ),
      { ok, state( PState, State ) };
    { ok, State, Extra } ->
      gen_server_pool:available( MgrPid, ProxyRef, self() ),
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
                     state       = S } = PState ) ->
  % we are switching out the pid in the From field with the pid of this
  % proxy, since if an embedded gen_server is using gen_server:reply/2
  % and thus using the noreply forms, we don't want to make the resource
  % available.
  case M:handle_call( Msg, { self(), FromRef }, S ) of
    { reply, Reply, NewS } ->
      gen_server_pool:available( MgrPid, ProxyRef, self() ),
      { reply, Reply, state( PState, NewS ) };
    { reply, Reply, NewS, Extra } ->
      gen_server_pool:available( MgrPid, ProxyRef, self() ),
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


handle_cast( Msg,
             #state{ manager_pid = MgrPid,
                     proxy_ref   = ProxyRef,
                     module      = M,
                     state       = S } = PState ) ->
  case M:handle_cast( Msg, S ) of
    { noreply, NewS } ->
      gen_server_pool:available( MgrPid, ProxyRef, self() ),
      { noreply, state( PState, NewS ) };
    { noreply, NewS, Extra } ->
      gen_server_pool:available( MgrPid, ProxyRef, self() ),
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
                     noreply_target = {Target,Tag}
                   } = PState ) ->
  Target ! { Tag, ProxyMsg },
  gen_server_pool:available( MgrPid, ProxyRef, self() ),
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


%%--------------------------------------------------------------------
%%% Tests
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
