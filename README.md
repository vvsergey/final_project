# Итоговый проект "Разработка систем анализа больших данных"

## Инструкция по запуску Airflow на Docker
1. Установите Docker Desktop с официального сайта https://www.docker.com/products/docker-desktop/
2. Создайте рабочую директорию в VS Code
3. Сколнируйте проект с Git
4. Соберите образ и запустите образ ```docker-compose -f docker-compose.yaml up -d```
5. По необходимости создайте нового пользователя.
    для этого перейдите в docker на экран Containers и в final_project-airflow-webserver-1 на вкладке 'Exec' выполните bash команду 
    ```airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@example.com --password admin```

## Работа с веб-интерфейсом Airflow
1. Откройте браузер и перейдите по адресу ```http://localhost:8080```
2. Используйте для доступа к интерфейсу реквизиты заданные в п.5
3. В случае необходимости изменения даты расчёта флагов активности param date
        перейдтите  `Admin > Variables` и замените значение переменной `PARAM_DATE` в `List Variable` на нужное.
4. Запустите DAG `final_dag`
5. После окончания работы пайплайна можно получить в файле `flags_activity.csv` папка проекта `data`


## Процесс ETL
1. **Extract (Извлечение)**
Данные загружаются из файла `profit_table.csv`.

2. **Transform (Трансформация)**
Трансформация данных с использованием предоставленного скрипта `transform_script.py`.

3. **Load (Загрузка)**
Результаты сохраняются в файл `flags_activity.csv`. Новые данные добавляются к существующим без перезаписи.

