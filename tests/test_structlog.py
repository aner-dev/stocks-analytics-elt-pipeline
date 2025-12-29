import structlog

log = structlog.get_logger()

log.warning(
    "Prueba de configuración de structlog", test="verificacion_formato", numero_prueba=1
)
