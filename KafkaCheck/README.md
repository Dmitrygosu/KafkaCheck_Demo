# KafkaCheck

Демонстрация работы с Apache Kafka на Go.

## Быстрый старт

📖 **[Подробная инструкция по тестированию](docs/MANUAL.md)**

```bash
# Запустить Kafka
make kafka-up

# Подождать ~30 секунд

# Запустить приложение
make run

# Открыть Kafka UI
# http://localhost:8080
```

## Makefile команды

| Команда | Описание |
|---------|----------|
| `make build` | Собрать бинарник |
| `make run` | Собрать и запустить |
| `make kafka-up` | Запустить Kafka (KRaft) |
| `make kafka-down` | Остановить Kafka |
| `make kafka-reset` | Удалить все данные Kafka |
| `make kafka-logs` | Логи Kafka |
| `make kafka-zk-up` | Запустить Kafka (Zookeeper) |
| `make help` | Все команды |

## KRaft vs Zookeeper

### KRaft (рекомендуется)
```bash
make kafka-up
```

**Преимущества:**
- Не требует отдельного сервиса Zookeeper
- Быстрее запускается (меньше контейнеров)
- Проще развёртывание и поддержка
- Лучше масштабируется
- Это будущее Kafka (Zookeeper deprecated с версии 3.5)

### Zookeeper (legacy)
```bash
make kafka-zk-up
```

**Когда использовать:**
- Старые версии Kafka (< 3.3)
- Уже существующая инфраструктура с Zookeeper
- Специфичные требования совместимости

## Структура проекта

```
├── cmd/server/main.go      # Точка входа
├── internal/
│   ├── app/app.go          # Логика приложения
│   ├── config/             # Конфигурация
│   ├── domain/             # Бизнес-модели
│   └── repository/         # Работа с Kafka
├── pkg/api/                # Protobuf сериализация
├── configs/config.yaml     # Настройки
├── docs/                   # Документация
├── Makefile               
├── docker-compose.yml      # KRaft
└── docker-compose.zookeeper.yml
```

## Конфигурация

Все настройки в `configs/config.yaml` с подробными комментариями.

Основные параметры:

| Параметр | Описание |
|----------|----------|
| `kafka.producer.required_acks` | Гарантия доставки: `NoResponse`, `WaitForLocal`, `WaitForAll` |
| `kafka.producer.compression` | Сжатие: `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `kafka.consumer.initial_offset` | С какого сообщения читать: `oldest`, `newest` |
| `kafka.consumer.rebalance_strategy` | Распределение партиций: `range`, `roundrobin`, `sticky` |

## Kafka UI

После запуска доступен по адресу **http://localhost:8080**

Возможности:
- Просмотр топиков и партиций
- Чтение сообщений
- Мониторинг consumer groups
- Управление конфигурацией

## Troubleshooting

**Kafka не запускается:**
```bash
make kafka-reset
make kafka-up
```

**Порты заняты:**
```bash
netstat -ano | findstr "9092"
docker ps
```

**Consumer не получает сообщения:**
- Проверь `initial_offset: oldest` в config.yaml
- Убедись что топик существует в Kafka UI

## Документация

- [Словарь терминов Kafka](docs/KAFKA_GLOSSARY.md)
