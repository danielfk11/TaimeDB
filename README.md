# TaimeDB

Banco de dados temporal orientado a commits.

Igual Git, mas para dados JSON.

## Visao

No TaimeDB, toda alteracao gera um commit com diff, timestamp e parent.
Nada e sobrescrito sem historico.

Principios:

- estado atual e historico completo
- rollback como operacao nativa
- snapshots periodicos para acelerar reconstrucoes
- realtime por websocket

## MVP implementado

- collections e documentos JSON
- commit por alteracao (`create`, `update`, `rollback`, `merge`)
- historico navegavel por documento
- consulta por commit e por timestamp (`at=RFC3339`)
- diff entre commits
- rollback por commit
- snapshots periodicos por documento
- websocket para assinaturas por `collection/document`
- branches e merge por documento
- replay de WAL no startup
- storage real com PebbleDB
- CLI: `commit`, `log`, `diff`, `rollback`, `branch`, `merge`

## Arquitetura atual

Client -> REST API -> Temporal Engine -> Storage Engine -> WAL/Disk

Componentes:

- `internal/api`: camada HTTP e websocket
- `internal/temporal`: regras de commit/diff/history/rollback/time travel/branch/merge
- `internal/commit`: modelo e repositorio in-memory de commits
- `internal/snapshot`: criacao e consulta de snapshots periodicos
- `internal/wal`: write-ahead log em JSON lines
- `internal/storage`: engine in-memory e engine Pebble para estados atuais (heads)
- `internal/realtime`: hub pub/sub para eventos de atualizacao
- `cmd/taimedb`: servidor
- `cmd/taime`: CLI cliente

## Estrutura de pastas

```text
cmd/
	taime/
	taimedb/
internal/
	api/
	commit/
	realtime/
	snapshot/
	storage/
	temporal/
	wal/
pkg/
	client/
```

## Requisitos

- Go 1.22+

## Como rodar

1. Instale dependencias:

```bash
go mod tidy
```

2. Suba o servidor (Pebble + replay de WAL por padrao):

```bash
go run ./cmd/taimedb \
	--addr :8080 \
	--wal data/taimedb.wal \
	--storage pebble \
	--storage-path data/pebble \
	--snapshot-interval 10
```

Backend em memoria (debug):

```bash
go run ./cmd/taimedb --storage memory
```

3. Use a CLI em outro terminal:

```bash
# cria/atualiza documento no branch main e gera commit
go run ./cmd/taime commit users 1 '{"name":"Joao","plan":"free"}' main

# novo commit
go run ./cmd/taime commit users 1 '{"name":"Joao","plan":"pro"}' main

# cria branch feature a partir de main
go run ./cmd/taime branch users 1 feature main

# commit no branch feature
go run ./cmd/taime commit users 1 '{"name":"Joao","plan":"enterprise"}' feature

# merge feature -> main
go run ./cmd/taime merge users 1 feature main

# historico
go run ./cmd/taime log users 1 main
```

Se quiser apontar para outro host:

```bash
export TAIME_ADDR=http://localhost:8080
```

## API HTTP

### Criar/atualizar documento

```bash
curl -X PUT http://localhost:8080/collections/users/1 \
	-H 'content-type: application/json' \
	-d '{"name":"Joao","plan":"free"}'
```

### Estado atual

```bash
curl http://localhost:8080/collections/users/1
```

Estado atual em branch especifico:

```bash
curl 'http://localhost:8080/collections/users/1?branch=feature'
```

### Estado em commit especifico

```bash
curl 'http://localhost:8080/collections/users/1?commit=a1'
```

### Estado em tempo especifico

```bash
curl 'http://localhost:8080/collections/users/1?at=2026-01-01T10:00:00Z'
```

### Historico

```bash
curl http://localhost:8080/history/users/1
```

Historico em branch especifico:

```bash
curl 'http://localhost:8080/history/users/1?branch=feature'
```

### Criar branch

```bash
curl -X POST 'http://localhost:8080/branches/users/1/feature?from=main'
```

### Listar heads de branch

```bash
curl http://localhost:8080/branches/users/1
```

### Merge entre branches

```bash
curl -X POST 'http://localhost:8080/merge/users/1?from=feature&to=main'
```

### Diff entre commits

```bash
curl http://localhost:8080/diff/a1/a2
```

### Rollback

```bash
curl -X POST http://localhost:8080/rollback/a1
```

## Realtime websocket

Assinatura por documento:

```text
GET /ws?collection=users&document=1&branch=main
```

Evento recebido:

```json
{
	"event": "update",
	"commit": "a5",
	"collection": "users",
	"document": "1",
	"timestamp": "2026-01-01T10:00:00Z",
	"changes": {
		"plan": "enterprise"
	}
}
```

## Estado do storage no MVP

- commits/historico em memoria (reconstruidos por WAL no startup)
- heads por branch em memoria
- snapshots em memoria
- WAL persistido em `data/taimedb.wal`
- estados atuais persistidos em Pebble (`data/pebble`) quando `--storage pebble`

## Testes automatizados

Executar:

```bash
go test ./...
```

Cobertura atual inclui:

- fluxo temporal com branch/merge
- replay de WAL
- handlers HTTP para lifecycle de documento, branch e merge

## Roadmap

V2:

- indexing
- compaction
- query language

V3:

- replication
- raft
- modo distribuido
- CRDT
- temporal SQL

