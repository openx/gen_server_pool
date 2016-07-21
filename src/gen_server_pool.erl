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
          stop/1,
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

-define(M, 1000000).

-type option() ::
        { 'pool_id', atom() | string() } |
        { 'prog_id', atom() | string() } |
        { 'min_pool_size', non_neg_integer() } |
        { 'max_pool_size', pos_integer() } |
        { 'max_worker_age_ms', pos_integer() | 'infinity' } |
        { 'max_worker_idle_ms', pos_integer() | 'infinity' } |
        { 'max_queued_requests', non_neg_integer() } |
        { 'max_request_wait_ms', pos_integer() | 'infinity' } |
        { 'sup_max_r', non_neg_integer() } |
        { 'sup_max_t', pos_integer() } |
        { 'mondemand', term() }.
-type options() :: list( option() ).

-record(worker, { pid :: pid(),
                  available_time :: erlang:timestamp(),
                  start_time :: erlang:timestamp() }).

-record(request, { call_args :: term(),
                   arrival_time :: erlang:timestamp() }).

-record(state, { proxy_ref,
                 sup_pid,
                 sup_max_r,
                 sup_max_t = 1,
                 starter_ref :: gen_server_pool_starter:starter_ref(),
                 available = [] :: list( #worker{} ),
                 requests  = queue:new() :: queue:queue( #request{} ),
                 min_pool_size  = 0 :: non_neg_integer(),
                 max_pool_size  = 10 :: pos_integer(),
                 max_worker_age_ms = infinity,
                 max_worker_idle_ms = infinity,
                 max_queued_requests = infinity,
                 max_request_wait_ms = infinity,
                 num_queued_requests = 0,
                 num_dropped_requests = 0,
                 module :: atom(),
                 pool_id,
                 prog_id,
                 wm_size = 0,
                 wm_active = 0,
                 wm_requests = 0 }).

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

stop( MgrPid ) ->
  gen_server:call( MgrPid, stop ).

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
  { ok, StarterRef } = gen_server_pool_starter:start_link( SupPid, [] ),

  % schedule periodic processing and start workers
  setup_schedule( S#state{ sup_pid = SupPid, starter_ref = StarterRef }, PoolOpts ).


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
  NewState = worker_unavailable( Pid, State ),
  { reply, {ok, Pid}, NewState };

handle_call( stop, _From, State ) ->
  { stop, normal, ok, State };

handle_call( Call, From, State ) ->
  NewState = handle_request( { '$gen_call', From, Call }, State ),
  { noreply, NewState }.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast( { ProxyRef, worker_available, Worker=#worker{} },
             State = #state{ proxy_ref = ProxyRef } ) ->
  NewState = worker_available( Worker, State ),
  { noreply, NewState };

handle_cast( Cast, State ) ->
  NewState = handle_request( { '$gen_cast', Cast }, State ),
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
  NewState = check_idle_timeouts( State ),
  schedule_idle_check( State ),
  { noreply, NewState };

handle_info( Info, State ) ->
  NewState = handle_request( Info, State ),
  { noreply, NewState }.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate( _Reason, #state{ starter_ref = StarterRef } ) ->
  %% FIXME: The gen_server_pool_sup is linked, and so it will exit
  %% automatically, but we need to kill the gen_server_pool_starter.  It would
  %% be better to use a proper supervision tree.
  gen_server_pool_starter:stop( StarterRef ),
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

timestamped_request(Req) ->
  #request{call_args=Req, arrival_time=os:timestamp()}.

stats( #state{ starter_ref = StarterRef,
               available = Workers,
               wm_size = WmSize, 
               wm_active = WmActive, 
               wm_requests = WmRequests,
               num_queued_requests = NumQueuedRequests,
               num_dropped_requests = NumDroppedRequests
               } ) ->
  Size   = gen_server_pool_starter:worker_count( StarterRef ),
  Idle   = length( Workers ),
  Active = Size - Idle,
  WmIdle = WmSize - WmActive,
  [ { size, Size },
    { active, Active },
    { idle, Idle },
    { tasks, NumQueuedRequests },
    { wmsize, WmSize },
    { wmactive, WmActive },
    { wmidle, WmIdle },
    { wmtasks, WmRequests },
    { drops, NumDroppedRequests }
  ].

collect_stats ( State = #state{ starter_ref = StarterRef,
                                available = Workers,
                                num_queued_requests = NumQueuedRequests,
                                wm_size = WmSize,
                                wm_active = WmActive,
                                wm_requests = WmRequests } ) ->
  Size   = gen_server_pool_starter:worker_count( StarterRef ),
  Idle   = length( Workers ),
  Active = Size - Idle,

  State#state{
    wm_size = max( Size, WmSize ),
    wm_active = max( Active, WmActive ),
    wm_requests = max( NumQueuedRequests, WmRequests ) }.

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
     wm_requests = 0,
     num_dropped_requests = 0 }.


-spec handle_request(term(), #state{}) -> #state{}.
%% Handle a gen_server call, cast, or info request by proxying it to a
%% gen_server_pool_proxy worker.
handle_request( Request, State ) ->
  State1 = check_for_worker( State ),
  State2 = time_out_requests( State1 ),
  State3 = enqueue_request( State2, Request ),
  do_work( State3 ).

-spec worker_available(#worker{}, #state{}) -> #state{}.
%% Return a gen_server_pool_proxy worker to the pool.
worker_available( Worker, State ) ->
  State1 = return_worker_to_pool( State, Worker ),
  State2 = time_out_requests( State1 ),
  do_work( State2 ).

-spec worker_unavailable(pid(), #state{}) -> #state{}.
%% Remove a gen_server_pool_proxy worker from the pool.  This function should
%% not normally be called.
worker_unavailable( Pid, State = #state{ available = Workers } ) ->
  State#state{ available = lists:keydelete( Pid, #worker.pid, Workers ) }.


-spec do_work(#state{}) -> #state{}.
do_work( State = #state{ num_queued_requests = 0 } ) ->
  %% No requests -- do nothing.
  State;

do_work( State = #state{ available = [] } ) ->
  %% No workers -- do nothing.
  State;

do_work( State = #state{ available = [ #worker{ pid = Pid } | Workers ],
                         requests = Requests,
                         num_queued_requests = NumQueuedRequests } ) ->
  { { value, #request{ call_args = CallArgs } }, RequestsOut } = queue:out( Requests ),
  case is_process_alive( Pid ) of
    false ->
      %% The first worker in the available stack may have died between when it
      %% was checked in check_for_worker and now.
      do_work( State#state{ available = Workers } );
    true  ->
      erlang:send( Pid, CallArgs, [noconnect] ),
      State#state{ available = Workers,
                   requests  = RequestsOut,
                   num_queued_requests = NumQueuedRequests - 1 }
  end.


ensure_min_pool_size( #state{ starter_ref = StarterRef, min_pool_size = MinPoolSize }, Synchronicity ) ->
  gen_server_pool_starter:ensure_min_workers( StarterRef, MinPoolSize, Synchronicity ).


schedule_idle_check( #state{ max_worker_idle_ms = infinity } ) -> ok;
schedule_idle_check( #state{ proxy_ref = ProxyRef, max_worker_idle_ms = MaxWorkerIdleMS } ) ->
  erlang:send_after( MaxWorkerIdleMS, self(), { ProxyRef, check_idle_timeouts } ).


check_idle_timeouts( #state{ available = [] } = S ) ->
  S;
check_idle_timeouts( #state{ proxy_ref = ProxyRef,
                             starter_ref = StarterRef,
                             max_worker_idle_ms = MaxWorkerIdleMS,
                             available = Available,
                             min_pool_size = MinPoolSize } = S ) ->
  MaxAgeTimeKill = aged_worker_kill_time( S ),
  Survivors0 = kill_aged_workers( ProxyRef, MaxAgeTimeKill, Available, [] ),

  PoolSize = gen_server_pool_starter:worker_count( StarterRef ),
  MaxWorkersToKill = PoolSize - MinPoolSize,
  MaxIdleTimeKill = now_sub( os:timestamp(), MaxWorkerIdleMS * 1000 ),
  Survivors = kill_idle_workers( ProxyRef, MaxIdleTimeKill, Survivors0, [], MaxWorkersToKill ),

  State = S#state{ available = Survivors },
  ensure_min_pool_size( State, async ),

  State.


%% Checks the worker pool for an available worker.  If none is found, and the
%% total number of workers is less than the maximum, starts a new worker.
%% Returns the updated state.
check_for_worker( State = #state{ available = Available } ) ->
  AgedWorkerKillTime = aged_worker_kill_time( State ),
  AvailableOut = check_for_worker_do( State, AgedWorkerKillTime, Available ),
  State#state{ available = AvailableOut }.

check_for_worker_do( State = #state{ proxy_ref = ProxyRef }, AgedWorkerKillTime,
                     Available = [ Worker = #worker{ pid = Pid } | RemainingWorkers ] ) ->
  case is_process_alive( Pid ) of
    false     ->
      %% Worker is dead; drop it and try again.
      check_for_worker_do( State, AgedWorkerKillTime, RemainingWorkers );
    true ->
      case worker_too_old( Worker, AgedWorkerKillTime ) of
        true ->
          %% Worker is alive, but too old; kill it and try again.
          gen_server_pool_proxy:stop( Pid, ProxyRef ),
          check_for_worker_do( State, AgedWorkerKillTime, RemainingWorkers );
        false ->
          %% Worker is alive and not too old; return.
          Available
      end
  end;
check_for_worker_do( #state{ starter_ref = StarterRef, min_pool_size = MinPoolSize, max_pool_size = MaxPoolSize },
                     _AgedWorkerKillTime, [] ) ->
  %% No workers are available.  If the total number of workers is less than
  %% the maximum, start a new one.
  gen_server_pool_starter:add_worker( StarterRef, MinPoolSize, MaxPoolSize, async ),
  [].


%% Returns a worker to the pool, unless it is too old, in which case it is
%% killed.
return_worker_to_pool( State = #state{ proxy_ref = ProxyRef, available = Available }, Worker = #worker{ pid = Pid } ) ->
  AgedWorkerKillTime = aged_worker_kill_time( State ),
  case worker_too_old( Worker, AgedWorkerKillTime ) of
    true ->
      %% The worker is too old; kill it.
      gen_server_pool_proxy:stop( Pid, ProxyRef ),
      ensure_min_pool_size( State, async ),
      State;
    false ->
      %% If a child sent a message to itself then it could already be in the list.
      case lists:keyfind( Pid, #worker.pid, Available ) of
        false -> State#state{ available = [ Worker | Available ] };
        _     -> State
      end
  end.


time_out_requests( State = #state{ requests = Requests,
                                   num_queued_requests = NumQueuedRequests,
                                   num_dropped_requests = NumDroppedRequests } ) ->
  RequestTimeoutTime = request_timeout_time( State ),
  { Requests1, NumQueuedRequests1, NumDroppedRequests1 } =
    time_out_requests_do( Requests, NumQueuedRequests, NumDroppedRequests, RequestTimeoutTime ),
  State#state{ requests = Requests1, num_queued_requests = NumQueuedRequests1, num_dropped_requests = NumDroppedRequests1 }.

time_out_requests_do( Requests, NumQueuedRequests, NumDroppedRequests, RequestTimeoutTime ) ->
  case queue:peek( Requests ) of
    empty ->
      { Requests, NumQueuedRequests, NumDroppedRequests };
    { value, Request = #request{ call_args = CallArgs } } ->
      case request_too_old( Request, RequestTimeoutTime ) of
        true ->
          case CallArgs of
            { '$gen_call', From, _ } ->
              gen_server:reply( From, { error, request_timeout } );
            _ -> %% cast or info.
              ok
          end,
          time_out_requests_do( queue:drop( Requests ), NumQueuedRequests - 1, NumDroppedRequests + 1, RequestTimeoutTime );
        false -> { Requests, NumQueuedRequests, NumDroppedRequests }
      end
  end.


enqueue_request( State = #state{ requests = Requests,
                                 max_queued_requests = MaxQueuedRequests,
                                 num_queued_requests = NumQueuedRequests,
                                 num_dropped_requests = NumDroppedRequests },
                 Request ) ->


  { Requests2, NumQueuedRequests2, NumDroppedRequests2 } =
    if NumQueuedRequests =:= 0 orelse NumQueuedRequests < MaxQueuedRequests ->
        { Requests, NumQueuedRequests, NumDroppedRequests };
       true ->
        { { value, #request{ call_args = CallArgs } }, Requests1 } = queue:out( Requests ),
        case CallArgs of
          { '$gen_call', From, _ } ->
            gen_server:reply( From, { error, request_dropped } );
          _ -> %% cast or info.
            ok
        end,
        { Requests1, NumQueuedRequests - 1, NumDroppedRequests + 1 }
    end,
  State#state{ requests = queue:in( timestamped_request( Request ), Requests2 ),
               num_queued_requests = NumQueuedRequests2 + 1,
               num_dropped_requests = NumDroppedRequests2 }.



%% Enforce max_worker_age and prune the worker list of dead processes.  This
%% function reverses the order of the worker list; it will be reversed again
%% by kill_idle_workers.  maintaining the order of the worker list.
kill_aged_workers( _ProxyRef, _TimeKill, [], Survivors ) ->
  Survivors;
kill_aged_workers( ProxyRef, TimeKill, [ Worker = #worker{ pid = Pid } | Available ], Survivors ) ->
  case is_process_alive( Pid ) of
    false     -> kill_aged_workers( ProxyRef, TimeKill, Available, Survivors );                 % Worker is dead; drop it.
    true ->
      case worker_too_old( Worker, TimeKill ) of
        true  -> gen_server_pool_proxy:stop( Pid, ProxyRef ),                                   % Worker is alive, but too old; kill it.
                 kill_aged_workers( ProxyRef, TimeKill, Available, Survivors );
        false -> kill_aged_workers( ProxyRef, TimeKill, Available, [ Worker | Survivors ] )     % Worker is alive and not too old; keep it.
      end
  end.

%% Kill up to MaxWorkersToKill workers that have not been active since
%% KillTime.
%%
%% We would prefer to kill the oldest workers, because those would be the
%% first to be killed by kill_aged_workers.  We could do that by using a heap
%% to track the MaxWorkersToKill oldest workers, but that is not implemented
%% here.  Instead, we use the reversed worker list returned by
%% kill_aged_workers; the workers that have been idle the longest are at the
%% beginning of thise list, and we hope idle time is a weak proxy for age.
%% This function reverses the order of the list again, meaning that the
%% combination of kill_aged_workers and kill_idle_workers will leave the list
%% in the original order.
kill_idle_workers( _ProxyRef, _TimeKill, Available, Survivors, MaxWorkersToKill) when MaxWorkersToKill =< 0 ->
  lists:reverse( Survivors, Available );
kill_idle_workers( _ProxyRef, _TimeKill, [], Survivors, _MaxWorkersToKill) ->
  lists:reverse( Survivors );
kill_idle_workers( ProxyRef, TimeKill, [ Worker = #worker{ pid = Pid, available_time = IdleTime } | Available ], Survivors, MaxWorkersToKill ) ->
  case timer:now_diff( TimeKill, IdleTime ) > 0 of
    true  -> gen_server_pool_proxy:stop( Pid, ProxyRef ),                                               % Worker has been idle too long; kill it.
             kill_idle_workers( ProxyRef, TimeKill, Available, Survivors, MaxWorkersToKill - 1 );
    false -> kill_idle_workers( ProxyRef, TimeKill, Available, [ Worker | Survivors], MaxWorkersToKill )% Worker is ok; keep it.
  end.


aged_worker_kill_time( #state{ max_worker_age_ms = infinity } ) ->
  undefined;
aged_worker_kill_time( #state{ max_worker_age_ms = MaxWorkerAgeMS } ) ->
  now_sub( os:timestamp(), MaxWorkerAgeMS * 1000 ).


request_timeout_time( #state{ max_request_wait_ms = infinity } ) ->
  undefined;
request_timeout_time( #state{ max_request_wait_ms = MaxRequestWaitMS } ) ->
  now_sub( os:timestamp(), MaxRequestWaitMS * 1000 ).


worker_too_old( _Worker, undefined ) ->
  false;
worker_too_old( #worker{ start_time = StartTime }, AgedWorkerKillTime ) ->
  timer:now_diff( AgedWorkerKillTime, StartTime ) > 0.


request_too_old( _Request, undefined ) ->
  false;
request_too_old( #request{ arrival_time = ArrivalTime }, RequestTimeoutTime ) ->
  timer:now_diff( RequestTimeoutTime, ArrivalTime ) > 0.


now_sub( { Megas, Secs, Micros }, SubMicros ) ->
  SubMicros0 = SubMicros rem ?M,
  SubSecs0 = SubMicros div ?M,
  {MicrosOut, SubSecs1} =
    case Micros - SubMicros0 of
      MicrosOut0 when MicrosOut0 >= 0 -> {MicrosOut0, SubSecs0};
      MicrosOut0                      -> {MicrosOut0 + ?M, SubSecs0 + 1}
    end,
  SubSecs2 = SubSecs1 rem ?M,
  SubMegas2 = SubSecs1 div ?M,
  {SecsOut, SubMegas3} =
    case Secs - SubSecs2 of
      SecsOut0 when SecsOut0 >= 0 -> {SecsOut0, SubMegas2};
      SecsOut0                    -> {SecsOut0 + ?M, SubMegas2 + 1}
    end,
  {Megas - SubMegas3, SecsOut, MicrosOut}.


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

-spec parse_opts(options(), #state{}) -> #state{}.
parse_opts( [], State ) ->
  finalize( State );
parse_opts( [ { pool_id, V } | Opts ], State ) when is_atom( V ) ->
  parse_opts( [ { pool_id, atom_to_list( V ) } | Opts ], State );
parse_opts( [ { pool_id, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ pool_id = V } );
parse_opts( [ { prog_id, V } | Opts ], State ) ->
  parse_opts( Opts, State#state{ prog_id = V } );
parse_opts( [ { min_pool_size, V } | Opts ], State ) when is_integer(V), V >= 0 ->
  parse_opts( Opts, State#state{ min_pool_size = V } );
parse_opts( [ { max_pool_size, V } | Opts ], State ) when is_integer(V), V >= 1 ->
  parse_opts( Opts, State#state{ max_pool_size = V } );
parse_opts( [ { max_worker_age_ms, V } | Opts ], State ) when is_integer(V), V >= 1; V =:= 'infinity' ->
  parse_opts( Opts, State#state{ max_worker_age_ms = V } );
parse_opts( [ { max_worker_idle_ms, V } | Opts ], State ) when is_integer(V), V >= 1; V =:= 'infinity' ->
  parse_opts( Opts, State#state{ max_worker_idle_ms = V } );
parse_opts( [ { max_queued_requests, V } | Opts ], State ) when is_integer(V), V >= 0; V =:= 'infinity' ->
  parse_opts( Opts, State#state{ max_queued_requests = V } );
parse_opts( [ { max_request_wait_ms, V } | Opts ], State ) when is_integer(V), V >= 1; V =:= 'infinity' ->
  parse_opts( Opts, State#state{ max_request_wait_ms = V } );
parse_opts( [ { sup_max_r, V } | Opts ], State ) when is_integer(V), V >= 0 ->
  parse_opts( Opts, State#state{ sup_max_r = V } );
parse_opts( [ { sup_max_t, V } | Opts ], State ) when is_integer(V), V >= 1 ->
  parse_opts( Opts, State#state{ sup_max_t = V } );
parse_opts( [ { mondemand, _ } | Opts ], State ) ->
  % stats option is not set in state
  parse_opts( Opts, State );
%% Deprecated options names.
parse_opts( [ { max_queue, V } | Opts ], State ) when is_integer(V), V >= 0; V =:= 'infinity' ->
  parse_opts( Opts, State#state{ max_queued_requests = V } );
parse_opts( [ { idle_timeout, V } | Opts ], State ) when is_integer(V), V >= 1; V =:= 'infinity' ->
  parse_opts( Opts, State#state{ max_worker_idle_ms = V * 1000 } );
parse_opts( [ { max_worker_age, V } | Opts ], State ) when is_integer(V), V >= 1; V =:= 'infinity' ->
  parse_opts( Opts, State#state{ max_worker_age_ms = V  * 1000 } );
parse_opts( [ { request_max_wait, V } | Opts ], State ) when is_integer(V), V >= 1; V =:= 'infinity' ->
  parse_opts( Opts, State#state{ max_request_wait_ms = V } ).



finalize( #state{ sup_max_r = undefined, max_pool_size = Sz } = S ) ->
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
  case ensure_min_pool_size( State, sync ) of
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

now_sub_test() ->
  ?assertEqual( { 1, 2, 3 },            now_sub( { 1, 2, 4 }, 1 )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 1, 3, 3 }, ?M )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 1, 3, 4 }, ?M + 1 )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 1, 3, 2 }, ?M - 1 )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 2, 2, 3 }, ?M * ?M )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 2, 2, 4 }, ?M * ?M + 1 )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 2, 2, 2 }, ?M * ?M - 1 )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 2, 3, 3 }, ?M * ?M + ?M )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 2, 1, 3 }, ?M * ?M - ?M )),
  ?assertEqual( { 1, 2, 3 },            now_sub( { 2, 3, 4 }, ?M * ?M + ?M + 1 )),
  ?assertEqual( { 0, 0, ?M - 1 },       now_sub( {0, 1, 0}, 1 )),
  ?assertEqual( { 0, ?M - 1, 0 },       now_sub( {1, 0, 0}, ?M )),
  ?assertEqual( { 0, ?M - 1, ?M - 1 },  now_sub( {1, 0, 0}, 1 )).

-endif. %% TEST
