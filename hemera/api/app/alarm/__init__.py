from flask_restx.namespace import Namespace

alarm_namespace = Namespace(
    "Indexer Alarm",
    path="/",
    description="Indexer Alarm API",
)
