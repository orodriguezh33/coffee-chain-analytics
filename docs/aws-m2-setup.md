# M2 AWS Foundation - Guía de Conexión y Validación

Esta guía resume el paso a paso para conectar el proyecto con AWS de forma segura
y validar permisos antes de crear recursos (S3, Athena, IAM role).

## Regla de seguridad

- Nunca compartas por chat ni subas a Git:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - contenido de `.env`
- Si necesitas ayuda, comparte solo:
  - `aws sts get-caller-identity`
  - errores `AccessDenied`
  - nombre del bucket
  - ARN del usuario/rol (no es secreto)

## 1. Crear usuario IAM para el proyecto

Ruta sugerida en consola de AWS:

- `IAM` -> `Users` -> `Create user`

Configuración recomendada:

- User name: `coffee-chain-analytics-user`
- Console access: `Disabled` (no necesario para M2)

Permisos recomendados (Attach policies directly):

- `AmazonS3FullAccess`
- `AmazonAthenaFullAccess`
- `AWSGlueConsoleFullAccess`
- `IAMFullAccess` (opcional para crear el rol por CLI en M2 paso 2.4)

## 2. Crear Access Key (acceso programático)

Dentro del usuario IAM:

- `Security credentials` -> `Create access key`
- Tipo de uso: `CLI` / `Command Line Interface`

Guardar inmediatamente:

- `Access key ID`
- `Secret access key` (solo se muestra una vez)

## 3. Configurar AWS CLI localmente

```bash
aws configure
```

Valores:

- `AWS Access Key ID`: tu key
- `AWS Secret Access Key`: tu secret
- `Default region name`: `us-east-1`
- `Default output format`: `json`

Verificacion inicial:

```bash
aws sts get-caller-identity
aws configure list
```

## 4. Actualizar `.env` local (sin commit)

Completa estas variables en `.env`:

```env
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

Notas:

- `.env` ya está protegido por `.gitignore`
- No pegues el archivo completo en ningún chat

## 5. Ejecutar verificación de permisos (M2 Paso 2.0)

Usa el script del proyecto:

```bash
docker compose --env-file .env run --rm \
  -v $(pwd)/scripts:/app/scripts \
  ingestion \
  python /app/scripts/aws/check_aws_permissions.py
```

Qué valida:

- S3 (list/acceso)
- Athena (workgroups/databases)
- Glue (Data Catalog)
- Redshift Serverless (solo listado)
- IAM (list roles)

## 6. Interpretar resultados

- Si `S3`, `Athena` y `Glue` están OK -> continuar con M2
- Si falta `IAM` -> puedes continuar y crear el rol por consola
- Si falta `S3`, `Athena` o `Glue` -> pedir permisos al admin antes de seguir

Mensaje sugerido al admin:

> Necesito permisos para el proyecto coffee-chain-analytics: S3, Athena y Glue Data Catalog (crear/listar/consultar), y opcionalmente IAM para crear un rol de servicio para Athena.

## 7. Crear bucket S3 (cuando los permisos ya estén listos)

Verifica primero si el nombre está libre. Si no, usa uno único:

- `coffee-chain-datalake-<iniciales>`

Ejemplo:

```bash
aws s3api create-bucket \
  --bucket coffee-chain-datalake-abc \
  --region us-east-1
```

Actualiza `.env` con el nombre real si cambia:

```env
S3_BUCKET=coffee-chain-datalake-abc
S3_BRONZE=s3://coffee-chain-datalake-abc/bronze
S3_SILVER=s3://coffee-chain-datalake-abc/silver
S3_GOLD=s3://coffee-chain-datalake-abc/gold
ATHENA_RESULTS=s3://coffee-chain-datalake-abc/athena-results/
```

## 8. Verificar configuración AWS completa (después de crear S3/Athena)

```bash
docker compose --env-file .env run --rm \
  -v $(pwd)/scripts:/app/scripts \
  ingestion \
  python /app/scripts/aws/verify_aws_setup.py
```

Qué valida:

- existencia del bucket S3
- escritura/borrado en S3
- existencia de la base `coffee_chain` en Athena
- ejecución de query de prueba en Athena

## 8.1. Versión automatizada (recomendada)

En vez de ejecutar cada comando manual de `aws`, puedes usar los scripts del repo
(idempotentes) para dejar trazabilidad del proceso.

### Paso A - S3 bucket + estructura de prefijos

```bash
docker compose --env-file .env run --rm \
  -v $(pwd)/scripts:/app/scripts \
  ingestion \
  python /app/scripts/aws/setup_s3_foundation.py
```

Este script:

- verifica si el bucket existe
- lo crea si no existe (usando `AWS_DEFAULT_REGION`)
- crea prefijos `bronze/`, `silver/`, `gold/`, `athena-results/`

### Paso B - Crear base de datos en Athena

```bash
docker compose --env-file .env run --rm \
  -v $(pwd)/scripts:/app/scripts \
  ingestion \
  python /app/scripts/aws/setup_athena_database.py
```

Este script:

- ejecuta `CREATE DATABASE IF NOT EXISTS`
- espera a que Athena termine
- valida que la DB exista en `AwsDataCatalog`

### Paso C - Crear rol IAM para Athena (opcional si tienes IAM)

```bash
docker compose --env-file .env run --rm \
  -v $(pwd)/scripts:/app/scripts \
  ingestion \
  python /app/scripts/aws/create_iam_role.py
```

Este script:

- crea (o reutiliza) `CoffeeChainDataRole`
- adjunta `AmazonS3FullAccess` y `AmazonAthenaFullAccess`
- imprime el ARN final para copiarlo a `.env`

## 9. Comandos que puedes compartir para soporte (seguros)

Puedes compartir estas salidas (no contienen secretos):

```bash
aws sts get-caller-identity
aws configure list
docker compose --env-file .env run --rm \
  -v $(pwd)/scripts:/app/scripts ingestion \
  python /app/scripts/aws/check_aws_permissions.py
```

## 10. Errores comunes

- `AccessDenied` en S3/Athena/Glue:
  - faltan permisos IAM en el usuario
- `BucketAlreadyExists`:
  - el nombre de bucket ya existe globalmente, usa uno único
- `Invalid ATHENA_RESULTS`:
  - revisa que `ATHENA_RESULTS` tenga formato `s3://bucket/prefix/`
- `No credentials`:
  - ejecuta `aws configure` y actualiza `.env`
