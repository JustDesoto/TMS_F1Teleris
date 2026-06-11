This project is a thesis on TMS courses

f1_pipeline/
├── extractors/          # Адаптеры под разные API (OpenF1, Ergast)
│   ├── base.py          # ABC с методами fetch(), transform_to_raw()
│   └── ergast.py
├── loaders/             
│   ├── mongo_loader.py  # сырые документы с метаданными (source_ts, hash)
│   └── ch_loader.py     # батчевая вставка в DDS
├── transformers/
│   ├── dds_transformer.py   # denormalization, SCD logic
│   └── validators.py        # Pydantic модели на границе слоев
├── models/
│   ├── raw/             # Pydantic модели для сырых JSON от API
│   ├── dds/             # Dataclasses/TypedDict для ClickHouse таблиц
│   └── mart/            # Агрегатные модели для витрин
├── repositories/        # Абстракции работы с БД
│   ├── mongo_repo.py    # CRUD с индексами (unique на (api, season, round, driver))
│   ├── pg_repo.py       # только для измерений и метаданных
│   └── ch_repo.py       # вставка с использованием native protocol
└── orchestrators/       # DAG-задачи (например, @task в Airflow)