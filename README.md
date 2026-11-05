# KafkaCheck

[![CI](https://github.com/Dmitrygosu/KafkaCheck_Demo/actions/workflows/ci.yaml/badge.svg)](https://github.com/Dmitrygosu/KafkaCheck_Demo/actions/workflows/ci.yaml)
![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)

Демо-стенд для Apache Kafka на Go (IBM Sarama): асинхронный продюсер и consumer group,
сообщения в Protobuf, кластер из трёх брокеров в docker compose (KRaft или ZooKeeper) и
Kafka UI, чтобы смотреть топики, партиции и смещения своими глазами.

Что можно потрогать через `configs/config.yaml`:

- гарантии доставки продюсера (`required_acks`) и повторы;
- сжатие (`none`, `gzip`, `snappy`, `lz4`, `zstd`) и батчинг;
- стратегии ребалансировки consumer group (`range`, `roundrobin`, `sticky`);
- начальное смещение и автокоммит;
- уровень и формат логов (`text` или `json`).

## Быстрый старт

Нужны Docker с Compose v2 и Go 1.27.

```bash
cd KafkaCheck && make kafka-up
```

```bash
make run
```

Приложение дождётся брокеров, отправит пачку сообщений и покажет, что получила consumer group.
Kafka UI — на http://localhost:8080. Подробности и сценарии экспериментов — в
[KafkaCheck/README.md](KafkaCheck/README.md) и [docs/MANUAL.md](KafkaCheck/docs/MANUAL.md).

## Устройство

| Пакет | Что делает |
|-------|-----------|
| `internal/repository` | продюсер (счётчики доставки без гонок, безопасный `Close`) и обработчик consumer group |
| `pkg/api` | Protobuf-схема сообщения и сериализация |
| `internal/config` | загрузка и полная валидация `config.yaml` |
| `internal/app` | сценарий демо и корректная остановка по Ctrl+C |

Тесты (`make test`) покрывают сериализацию, валидацию конфига, продюсер на моках Sarama,
включая `Close` во время отправки, и обработчик consumer group. CI запускает golangci-lint,
тесты с `-race` и govulncheck.
