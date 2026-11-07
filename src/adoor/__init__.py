"""ADoor for Python: shared PostgreSQL tables with per-identity access, a change history and undo.

    import adoor
    db = adoor.login(name="Growth curves notebook")   # or adoor.connect() with ADOOR_TOKEN set
    db.insert("plate_reads", [{"well": "A1", "od600": 0.42}])
    db.df("SELECT well, od600 FROM plate_reads ORDER BY well")
    db.history_df(table="plate_reads")                 # who changed what, and when
"""

from .auth import credentials_path, exchange, github_actions_token, login
from .client import DEFAULT_URL, VERSION, Client, Logger, Result, Sandbox, connect, quote_ident
from .errors import AdoorError
from .values import Json

__version__ = VERSION
__all__ = [
    "AdoorError", "Client", "DEFAULT_URL", "Json", "Logger", "Result", "Sandbox", "connect", "credentials_path",
    "exchange", "github_actions_token", "login", "quote_ident", "__version__",
]
