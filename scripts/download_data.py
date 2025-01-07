import os
import pandas as pd
from sklearn.datasets import load_iris

# Загружаем набор данных Iris sklearn
def download_data(output_path):
    
    iris = load_iris()
    df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
    df['species'] = iris.target
    df.to_csv(output_path, index=False)
   

# Cохраняем данные
download_data('data/iris_dataset.csv')