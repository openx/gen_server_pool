-module(gen_server_pool_starter).

-behaviour(gen_server).

-export([ start_link/2,
          add_worker/4,
          ensure_min_workers/3,
          worker_count/1,

          init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3 ]).

-record(state, { gen_server_pool_sup_pid }).

-type starter_ref() :: { pid(), pid() }.
-type synchronicity() :: 'sync' | 'async'.

-export_type( [ starter_ref/0 ] ).


%%%
%%% API
%%%

-spec start_link(pid(), list()) -> {ok, starter_ref() }.
start_link( GenServerPoolSupPid, Options ) ->
  { ok, Pid } = gen_server:start_link( ?MODULE, GenServerPoolSupPid, Options ),
  { ok, { Pid, GenServerPoolSupPid } }.


-spec add_worker(starter_ref(), MinPoolSize::non_neg_integer(), MaxPoolSize::pos_integer(), synchronicity()) -> 'ok' | { 'error', Reason::term() }.
%% @doc Starts a new worker unless doing so would bring the number of workers
%% above `MaxPoolSize'.  Also, if the number of workers is less than
%% `MinPoolSize', starts additional workers to bring it up to that level.  If
%% you never want to start more than one worker, pass `0' for `MinPoolSize'.
%%
%% If `Synchronicity' is `sync', do not return until the workers are started;
%% if `async', the workers will be started asynchronously and the function
%% will return immediately.
add_worker( { SelfPid, GenServerPoolSupPid }, MinPoolSize, MaxPoolSize, Synchronicity ) ->
  case Synchronicity of
    async -> gen_server:cast( SelfPid, { add_worker, MinPoolSize, MaxPoolSize } );
    sync  -> add_worker( MinPoolSize, MaxPoolSize, GenServerPoolSupPid )
  end.


-spec ensure_min_workers(starter_ref(), Count::pos_integer(), synchronicity()) -> 'ok' | { 'error', Reason::term() }.
%% @doc Ensures that at least `MinPoolSize' workers are alive by starting new
%% workers if necessary.
%%
%% If `Synchronicity' is `sync', do not return until the workers are started;
%% if `async', the workers will be started asynchronously and the function
%% will return immediately.
ensure_min_workers( { SelfPid, GenServerPoolSupPid }, MinPoolSize, Synchronicity ) when is_integer(MinPoolSize), MinPoolSize > 0 ->
  case Synchronicity of
    async -> gen_server:cast( SelfPid, { ensure_min_workers, MinPoolSize } );
    sync  -> ensure_min_workers( MinPoolSize, GenServerPoolSupPid )
  end;
ensure_min_workers( _StarterRef, 0, _Synchronicity ) ->
  ok.


-spec worker_count(starter_ref()) -> non_neg_integer().
%% @doc Returns the number of workers, both busy and idle, that are alive.
worker_count( { _, GenServerPoolSupPid } ) ->
  proplists:get_value( active, supervisor:count_children( GenServerPoolSupPid ) ).


%%%
%%% gen_server Callbacks
%%%

init( GenServerPoolSupPid ) ->
  { ok, #state{ gen_server_pool_sup_pid = GenServerPoolSupPid } }.

handle_call( request, _From, State) ->
  { reply, ok, State }.

handle_cast( { add_worker, MinPoolSize, MaxPoolSize }, State = #state{ gen_server_pool_sup_pid = GenServerPoolSupPid } ) ->
  add_worker( MinPoolSize, MaxPoolSize, GenServerPoolSupPid ),
  { noreply, State };

handle_cast( { ensure_min_workers, MinPoolSize }, State = #state{ gen_server_pool_sup_pid = GenServerPoolSupPid } ) ->
  ensure_min_workers( MinPoolSize, GenServerPoolSupPid ),
  discard_ensure_min_workers_messages(),
  { noreply, State }.

handle_info( info, State ) ->
  { noreply, State }.

terminate( _Reason, _State ) ->
  ok.

code_change( _OldVsn, State, _Extra ) ->
  { ok, State }.


%%%
%%% Internal Functions
%%%

add_workers( Count, GenServerPoolSupPid ) when Count > 0 ->
  case gen_server_pool_sup:add_child( GenServerPoolSupPid ) of
    { error, Error } -> Error;
    _                -> add_workers( Count - 1, GenServerPoolSupPid )
  end;
add_workers( 0, _GenServerPoolSupPid ) ->
  ok.

ensure_min_workers( MinPoolSize, GenServerPoolSupPid ) when MinPoolSize > 0 ->
  CurrentPoolSize = proplists:get_value( active, supervisor:count_children( GenServerPoolSupPid ) ),
  NumWorkersToAdd = MinPoolSize - CurrentPoolSize,
  NumWorkersToAdd > 0 andalso add_workers( NumWorkersToAdd, GenServerPoolSupPid );
ensure_min_workers( MinPoolSize, _GenServerPoolSupPid ) when MinPoolSize =:= 0 ->
  ok.

add_worker( MinPoolSize, MaxPoolSize, GenServerPoolSupPid ) ->
  CurrentCount = proplists:get_value( active, supervisor:count_children( GenServerPoolSupPid ) ),
  if CurrentCount < MinPoolSize -> add_workers( MinPoolSize - CurrentCount, GenServerPoolSupPid );
     CurrentCount < MaxPoolSize -> add_workers( 1, GenServerPoolSupPid );
     true                       -> ok
  end.

%% Discards any queued ensure_min_workers cast messages using selective
%% receive.
discard_ensure_min_workers_messages() ->
  receive
    { '$gen_cast', { ensure_min_workers, _ } } ->
      discard_ensure_min_workers_messages()
  after
    0 -> ok
  end.
