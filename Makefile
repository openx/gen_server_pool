REBAR:=rebar

.PHONY: all erl test clean doc check

all: erl

erl:
	$(REBAR) get-deps compile

check test: all
	@mkdir -p .eunit
	$(REBAR) skip_deps=true eunit

dialyzer: all
	dialyzer ebin

clean:
	$(REBAR) clean
	-rm -rvf deps ebin doc .eunit

doc:
	$(REBAR) doc

