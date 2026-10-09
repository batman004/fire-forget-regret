.PHONY: test vendors record record-fast cast quickstart

PY ?= python3

test:            ## run every test
	$(PY) -m unittest

vendors:         ## terminal 2 for the live demo
	$(PY) -m demo vendors

quickstart:      ## the smallest example; try DIE=job-2 make quickstart, twice
	$(PY) examples/quickstart.py

record:          ## replay the whole demo, typed, for a screen recording (~7 min)
	$(PY) scripts/record_demo.py

record-fast:     ## the same replay with no typing or pauses
	$(PY) scripts/record_demo.py --fast

cast:            ## record to demo.cast with asciinema (pip install asciinema)
	asciinema rec -c "$(PY) scripts/record_demo.py" demo.cast
