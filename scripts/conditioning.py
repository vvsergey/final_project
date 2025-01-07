import pandas as pd
from sklearn.preprocessing import StandardScaler

# Загрузить данные
def load(input):
    df = pd.read_csv(input)
    return df


# Удаляем пропущенные значения, если есть
def clean(df):
    df = df.dropna()
    return df


# Стандартизация (кроме колонки 'species')
def scale(df):
    scaler = StandardScaler()
    feature_columns = df.columns[:-1]
    df[feature_columns] = scaler.fit_transform(df[feature_columns])
    return df

# Преобразуем колонку species в колонку target
def create(df):
    df = df.rename(columns={'species':'target'})
    return df

# Сохранение данных
def save(df, output_path):
    df.to_csv(output_path, index=False)


def main(input_path, output_path):
    # Основной процесс обработки данных
    df = create(scale(clean(load(input_path))))
    save(df, output_path)


if __name__ == "__main__":
    main("data/iris_dataset.csv", "data/iris_dataset_cleaned.csv")