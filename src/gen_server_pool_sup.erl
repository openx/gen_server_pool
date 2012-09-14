%%%-------------------------------------------------------------------
%%% File    : gen_server_pool_sup.erl
%%% Author  : Joel Meyer <joel@openx.org>
%%% Description :
%%%   Supervisor for a pool of gen_servers.
%%%
%%% Created : 4 May 2011 by Joel Meyer <joel@openx.org>
%%%-------------------------------------------------------------------

-module(gen_server_pool_sup).

-behaviour(supervisor).

-export([ start_link/7,
          add_child/1,
          init/1 ]).

start_link( ManagerPid, ProxyRef, Module, Args, Options, MaxR, MaxT ) ->
  supervisor:start_link( ?MODULE,
                         [ ManagerPid, ProxyRef, Module, Args, Options, MaxR, MaxT ] ).

add_child( SupRef ) ->
  supervisor:start_child( SupRef, [] ).

init( [ ManagerPid, ProxyRef, Module, Args, Options, MaxR, MaxT ] ) ->
  { ok,
   { { simple_one_for_one, MaxR, MaxT },
     [ { pool_name( Module ),
         { gen_server_pool_proxy, start_link, [ ManagerPid, ProxyRef,
                                                Module, Args, Options ] },
         transient, 2000, worker, [ Module ] } ] } }.

pool_name( Module ) ->
  list_to_atom( atom_to_list( Module ) ++ "_pool_sup" ).
