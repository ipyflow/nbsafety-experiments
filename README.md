# Readme

There are two main functionalities in that this: scraping and replaying. You
don’t need to deal with scraping if using the already-scraped
[traces.sqlite](https://drive.google.com/file/d/1Yf_17-SESPVdyawfNbxF54VIbe4om2eX/view?usp=sharing)
sqlite database. Besides that there are two scripts worth
noting: one which replays a single notebook session, and one which replays
a whole set of sessions after filtering based on some criteria

# Replaying a single session

`replay-session.py` replays a single notebook session (given `trace_id` and
`session_id`, basically ids for the repository and per-repository session),
handling things like timeouts, figuring out packages that need installation,
etc. It also counts the number of exceptions that occurred during replay;
probably worth filtering out sessions where more than ~5-10% of the cell
executions give an exception. There’s also a bunch of ancillary stuff in there
that’s specific to nbsafety, like counting how often the user picks a stale
cell for re-execution or a refresher cell; if just using the replay functionality
and not replicating nbsafety results, this can just be deleted. Note that it assumes
availability of tables `replay_stats` and `replay_exception_stats` in the `traces.sqlite`
database whose schemas must be manually generated; the PyCharm sqlite connector is
pretty good for this.

# Replaying all sessions satisfying filtering criteria

`run-replay-experiments.py` runs all the sessions through a filtering process
and replays all sessions that pass a filter. A bunch of the filtering criteria
were manually specified after seeing nonsensical sessions that were replayed.
It also accepts a `--version` argument; if you specify the same version, it skips
sessions that were already replayed; if you specify a new version, it starts
from scratch. There are also some nbsafety-specific parameters:
- `--naive-refresher-computation`: is a baseline used in the paper,
- `--forward-only-propagation`: used to measure utility of highlights where new
  ones are only created in later cells (spacially relative to the currently
  executed one) instead of both earlier and later cells
- `no-nbsafety`: used to determine how much faster non-nbsafety replay was (to
  see what nbsafety overhead was like).
