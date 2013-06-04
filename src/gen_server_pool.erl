%%%-------------------------------------------------------------------
%%% File    : gen_server_pool.erl
%%% Author  : Joel Meyer <joel@openx.org>
%%% Description :
%%%   A pool for gen_servers.
%%%
%%% Created :  4 May 2011
%%%-------------------------------------------------------------------
-module(gen_server_pool).

-behaviour(gen_server).

%% API
-export([ start_link/4,
          start_link/5,
          get_stats/1,
          get_pool_pids/1,
          available/4,
          unavailable/2
        ]).

%% gen_server callbacks
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3 ]).

-record(state, { proxy_ref,
                 sup_pid,
                 sup_max_r,
                 sup_max_t = 1,
                 available = [],
                 requests  = { [], [] },
                 min_size  = 0,
                 max_size  = 10,
                 idle_secs = infinity,
                 max_queued_tasks = infinity,
                 num_queued_tasks = 0,
                 num_dropped_tasks = 0,
                 module,
                 pool_id,
                 prog_id,
                 wm_size = 0,
                 wm_active = 0,
                 wm_tasks = 0,
                 max_worker_age = infinity
                 }).
-record(worker, {
          pid, 
          available_time,
          start_time
}).
         

%%====================================================================
%% API
%%====================================================================
start_link( Module, Args, Options, PoolOpts ) ->
  gen_server:start_link( ?MODULE,
                         [ Module, Args, Options, PoolOpts ],
                         Options ).


start_link( Name, Module, Args, Options, PoolOpts ) ->
  gen_server:start_link( Name,
                         ?MODULE,
                         [ Module, Args, Options, PoolOpts ],
                         Options ).

get_stats( MgrPid ) ->
  gen_server:call( MgrPid, gen_server_pool_stats ).

available( MgrPid, ProxyRef, WorkerPid, WorkerStartTime ) ->
  Worker = #worker{ pid = WorkerPid, 
                    available_time = os:timestamp(), 
                    start_time = WorkerStartTime },
  gen_server:cast( MgrPid, { ProxyRef, worker_available, Worker } ).

unavailable( MgrPid, WorkerPid ) ->
  gen_server:call( MgrPid, { unavailable, WorkerPid } ).

get_pool_pids( MgrPid ) ->
  gen_server:call( MgrPid, get_pool_pids ).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init( [ Module, Args, Options, PoolOpts ] ) ->
  % Init state from opts
  S = parse_opts( PoolOpts, #state{ module = Module } ),

  % Get config from state
  #state{ proxy_ref = ProxyRef, sup_max_r = MaxR, sup_max_t = MaxT } = S,

  % Start supervisor for pool members
  { ok, SupPid } = gen_server_pool_sup:start_link( self(), ProxyRef,
                                                   Module, Args, Options,
                                                   MaxR, MaxT ),

  % schedule periodic processing and start workers
  setup_schedule( S#state{ sup_pid = SupPid }, PoolOpts ).


%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call( gen_server_pool_stats, _From, State ) ->
  { reply, stats( State ), State };

handle_call( get_pool_pids, _From, State = #state{ sup_pid = SupPid } ) ->
  Children = supervisor:which_children(SupPid),
  { reply, {ok, Children}, State };

handle_call( {unavailable, Pid}, _From, State ) ->
  NState = worker_unavailable( Pid, State ),
  { reply, {ok, Pid}, NState };

handle_call( Call, From, State ) ->
  case handle_request ({ '$gen_call', From, Call }, State) of
    {ok, NewState} -> { noreply, NewState };
    {Error, NewState} -> {reply, Error, NewState}
  end.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast( { ProxyRef, worker_available, Worker=#worker{} },
             State = #state{ proxy_ref = ProxyRef } ) ->
  {_, NewState} = worker_available (Worker, State),
  {noreply, NewState};

handle_cast( Cast, State ) ->
  { _, NewState } = handle_request ({'$gen_cast', Cast }, State ),
  { noreply, NewState }.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info( { ProxyRef, emit_stats },
             #state{ proxy_ref = ProxyRef } = State ) ->
  NewState = emit_stats( State ),
  schedule_emit_stats( State ),
  { noreply, NewState};

handle_info( { ProxyRef, collect_stats },
             #state{ proxy_ref = ProxyRef } = State ) ->
  NewState = collect_stats( State ),
  schedule_collect_stats( State ),
  { noreply, NewState };

handle_info( { ProxyRef, check_idle_timeouts },
             #state{ proxy_ref = ProxyRef } = State ) ->
  schedule_idle_check( State ),
  { noreply, check_idle_timeouts( State ) }; 

handle_info( Info, State ) ->
  { _, NewState } = handle_request (Info, State ),
  { noreply, NewState }.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate( Reason, State ) ->
  terminate_pool( Reason, State ),
  ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change( _OldVsn, State, _Extra ) ->
  { ok, State }.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
handle_request( Req, State = #state{ requests = { Push, Pop },
                                     num_queued_tasks=NumTasks } ) ->
  do_work( State#state{ requests = { [ Req | Push ], Pop }, 
                        num_queued_tasks=NumTasks+1} ).

worker_available( Worker = #worker{ pid = Pid }, 
                  State = #state{ available = Workers } ) ->
  % If a child sent a message to itself then it could already be in the list
  case proplists:is_defined( Pid, Workers ) of
    true  -> do_work( State );
    false -> do_work( State#state{ available = [ Worker | Workers ] } )
  end.


worker_unavailable( Pid, State = #state{ available = Workers } ) ->
  State#state{ available = proplists:delete( Pid, Workers ) }.

stats( #state{ sup_pid   = SupPid,
               available = Workers,
               wm_size = WmSize, 
               wm_active = WmActive, 
               wm_tasks = WmTasks,
               num_queued_tasks = NumTasks,
               num_dropped_tasks = NumDroppedTasks
               } ) ->
  Size   = proplists:get_value( active, supervisor:count_children( SupPid ) ),
  Idle   = length( Workers ),
  Active = Size - Idle,
  WmIdle = WmSize - WmActive,
  [ { size, Size },
    { active, Active },
    { idle, Idle },
    { tasks, NumTasks },
    { wmsize, WmSize},
    { wmactive, WmActive},
    { wmidle, WmIdle},
    { wmtasks, WmTasks},
    { drops, NumDroppedTasks}
  ].

collect_stats ( State = #state{ sup_pid = SupPid,
                                available = Workers,
                                wm_size = WmSize, wm_active = WmActive, 
                                wm_tasks = WmTasks,
                                num_queued_tasks=NumTasks } ) ->
  Size   = proplists:get_value( active, supervisor:count_children( SupPid ) ),
  Idle   = length( Workers ),
  Active = Size - Idle,
  Tasks  = NumTasks,

  NewWmSize = max (Size,WmSize),
  NewWmActive = max (Active, WmActive),
  NewWmTasks = max (Tasks, WmTasks),

  State#state{wm_size = NewWmSize, wm_active = NewWmActive, wm_tasks = NewWmTasks}.

emit_stats( #state{ prog_id = ProgId, pool_id = PoolId } = S ) ->
  N = fun( K ) -> 
        lists:flatten( [ PoolId, [ "_" | atom_to_list( K ) ] ] )
      end,

  M = lists:foldl( fun( { K, V }, A ) ->
                        [ { gauge, N( K ), V } | A ]
                   end,
                   [],
                   stats( S ) ),

  mondemand:send_stats( ProgId, [], M ),
  % reset stats after emit
  S#state{
     wm_size = 0,
     wm_active = 0,
     wm_tasks = 0,
     num_dropped_tasks=0
  }.

terminate_pool( _Reason, _State ) ->
  ok.

do_work( State = #state{ requests  = { [], [] } } ) ->
  % No requests - do nothing.
  {ok, State};

do_work( State = #state{ available = [],
                         requests  = { Push, [ _ | Pop ] },
                         max_size  = MaxSize,
                         sup_pid   = SupPid,
                         num_queued_tasks = NumTasks,
                         num_dropped_tasks = DroppedTasks,
                         max_queued_tasks = MaxTasks
                 } ) ->
  % Requests, but no workers - check if we can start a worker.
  PoolSize = proplists:get_value( active, supervisor:count_children( SupPid ) ),
  case PoolSize < MaxSize of
    true  ->
      gen_server_pool_sup:add_child( SupPid ),
      {ok, State};
    false -> 
      % we are at max pool size, so allow queuing to occur
      case MaxTasks =/= infinity andalso NumTasks > MaxTasks of
        true ->
          % queue too big, so drop the request
          { {error, request_dropped },
            State#state {
              requests = {Push, Pop},
              num_queued_tasks = NumTasks - 1,
              num_dropped_tasks = DroppedTasks + 1
            }
          };
        false ->
          { ok, State }
      end
  end;

do_work( State = #state{ requests  = { Push, [] } } ) ->
  do_work( State#state{ requests = { [], lists:reverse( Push ) } } );

do_work( State = #state{ proxy_ref = ProxyRef,
                         available = [ #worker{ pid = Pid, start_time = WorkerStartTime } | Workers ],
                         requests  = { Push, [ Req | Pop ] },
                         num_queued_tasks = NumTasks,
                         max_worker_age = MaxWorkerAge }) ->
  case is_process_alive(Pid) of
    false ->
      do_work( State#state{ available = Workers } );
    true  ->
      %% Check worker age here, and boot it if too old
      case worker_too_old(WorkerStartTime, MaxWorkerAge) of
        true -> 
          % Kill the old worker, and check the min pool size
          gen_server_pool_proxy:stop( Pid, ProxyRef ),
          assure_min_pool_size( State ),
          do_work( State#state { available = Workers } );
        false ->
          erlang:send( Pid, Req, [noconnect] ),
          {ok, State#state{ available = Workers,
                            requests  = { Push, Pop },
                            num_queued_tasks=NumTasks-1}
          }
       end
  end.

worker_too_old (_WorkerStartTime, infinity) ->
  false;
worker_too_old (WorkerStartTime, MaxWorkerAge) ->
  Now = os:timestamp(),
  WorkerAge = seconds_between( Now, WorkerStartTime ),
  WorkerAge >= MaxWorkerAge.
  

assure_min_pool_size( #state{ min_size = MinSize, sup_pid = SupPid } = S ) ->
  PoolSize = proplists:get_value( active, supervisor:count_children( SupPid ) ),
  add_children( MinSize - PoolSize, S ).


add_children( N, #state{} ) when N =< 0 ->
  ok;
add_children( N, #state{ sup_pid = SupPid } = S ) ->
  case gen_server_pool_sup:add_child( SupPid ) of
    { error, Error } -> Error;
    _                -> add_children( N-1, S )
  end.


schedule_idle_check( #state{ idle_secs = infinity } = S ) ->
  S;
schedule_idle_check( #state{ proxy_ref = ProxyRef,
                             idle_secs = IdleSecs } = S ) ->
  erlang:send_after( IdleSecs * 1000, self(), { ProxyRef, check_idle_timeouts } ),
  S.


check_idle_timeouts( #state{ available = [] } = S ) ->
  S;
check_idle_timeouts( #state{ proxy_ref = ProxyRef,
                             sup_pid = SupPid,
                             idle_secs = IdleSecs,
                             available = Available,
                             min_size = MinSize } = S ) ->
  PoolSize = proplists:get_value( active, supervisor:count_children( SupPid ) ),
  MaxWorkersToKill = PoolSize - MinSize,
  Survivors = kill_idle_workers (ProxyRef,IdleSecs,Available, [], MaxWorkersToKill),

  State = S#state{ available = Survivors },
  assure_min_pool_size( State ),

  State.

kill_idle_workers(_ProxyRef,_IdleSecs,Available, Survivors, MaxWorkersToKill)  when MaxWorkersToKill =< 0 ->
  Available ++ Survivors;
kill_idle_workers(_ProxyRef,_IdleSecs,[], Survivors, _MaxWorkersToKill) ->
  Survivors;
kill_idle_workers(ProxyRef,IdleSecs,[Worker=#worker{pid=Pid,available_time=Time} | Available], Survivors, MaxWorkersToKill) ->
  Now = os:timestamp(),
  IdleTime = seconds_between( Now, Time ),
  case IdleTime >= IdleSecs of
    true ->
      case is_process_alive(Pid) of
        false -> kill_idle_workers(ProxyRef,IdleSecs, Available, Survivors, MaxWorkersToKill);
        true ->
          gen_server_pool_proxy:stop( Pid, ProxyRef ),
          kill_idle_workers(ProxyRef,IdleSecs, Available, Survivors, MaxWorkersToKill-1)
      end;
    false ->
      kill_idle_workers(ProxyRef,IdleSecs, Available, [ Worker | Survivors], MaxWorkersToKill)
  end.

seconds_between( { MS1, S1, _ }, { MS1, S2, _ } ) ->
  S1 - S2;
seconds_between( { MS1, S1, _ }, { MS2, S2, _ } ) ->
  ( MS1 * 1000000 + S1 ) - ( MS2 * 1000000 + S2 ).


schedule_emit_stats( #state{ prog_id = undefined } ) ->
  % No prog_id, so can't emit stats
  ok;
schedule_emit_stats( #state{ pool_id = undefined } ) ->
  % No pool_id, so can't emit stats
  ok;
schedule_emit_stats( #state{ proxy_ref = ProxyRef } ) ->
  % pool_id and prog_id are set, can emit stats
  erlang:send_after( 60*1000, self(), { ProxyRef, emit_stats } ).

schedule_collect_stats( #state{ proxy_ref = ProxyRef } ) ->
  % collect stats every 10 second.
  erlang:send_after( 1*1000, self(), { ProxyRef, collect_stats } ).

parse_opts( [], State ) ->
  finalize( State );
parse_opts( [ { pool_id, V } | Opts ], State ) when is_atom( V ) ->
  parse_opts( [ { pool_id, atom_to_list( V ) } | Opts ], State );
parse_opts( [ { pool_id, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ pool_id = V } );
parse_opts( [ { prog_id, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ prog_id = V } );
parse_opts( [ { min_pool_size, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ min_size = V } );
parse_opts( [ { max_pool_size, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ max_size = V } );
parse_opts( [ { idle_timeout, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ idle_secs = V } );
parse_opts( [ { max_queue, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ max_queued_tasks = V } );
parse_opts( [ { max_worker_age, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ max_worker_age = V } );
parse_opts( [ { sup_max_r, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ sup_max_r = V } );
parse_opts( [ { sup_max_t, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ sup_max_t = V } );
parse_opts( [ { mondemand, _ } | Opts ], State ) ->
  % stats option is not set in state
  parse_opts( Opts, State ).


finalize( #state{ sup_max_r = undefined, max_size = Sz } = S ) ->
  % Set max_r to max pool size if not set
  finalize( S#state{ sup_max_r = Sz } );
finalize( State ) ->
  % Add unique reference for this proxy
  State#state{ proxy_ref = make_ref() }.


setup_schedule( State, PoolOpts ) ->
  % collect stats as default
  schedule_collect_stats( State ),

  % but make emission to mondemand optional
  case proplists:get_value( mondemand, PoolOpts, true ) of
    false -> ok;
    _ -> schedule_emit_stats( State )
  end,

  % start min_pool_size workers and schedule idle time check
  case assure_min_pool_size( State ) of
    ok ->
      schedule_idle_check( State ),
      { ok, State };
    Error ->
      { stop, Error }
  end.

%%--------------------------------------------------------------------
%%% Tests
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
