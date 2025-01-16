from flask_restx.namespace import Namespace

omega_namespace = Namespace(
    "Omega Namespace",
    path="/",
    description="Omega API",
)
