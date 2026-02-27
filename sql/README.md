# Estructura de `sql/`

Esta carpeta se organiza por dominio para mantener separado:
- SQL declarativo (DDL / validaciones)
- scripts Python que ejecutan o exportan datos

## Estructura actual

```text
sql/
├── athena/
│   ├── ddl/
│   │   ├── create_bronze_tables.sql
│   │   └── validate_joins.sql
│   └── scripts/
│       ├── run_ddl.py
│       └── generate_mapping_backlog.py
├── bi/
│   └── scripts/
│       └── export_powerbi_snapshot.py
└── README.md
```

## Convención

- `ddl/`: solo archivos `.sql`.
- `scripts/`: solo archivos `.py`.
- Evitar mezclar SQL y Python en el mismo subdirectorio.
- No versionar `__pycache__/` ni `.DS_Store`.
