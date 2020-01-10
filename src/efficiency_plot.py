import os
import pandas as pd 
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate

SCRIPT_DIRECTORY = os.path.realpath("")
HOME_DIRECTORY = os.path.split(SCRIPT_DIRECTORY)[0]
DATA_DIRECTORY = os.path.join(HOME_DIRECTORY, "data")
IMAGE_DIRECTORY = os.path.join(HOME_DIRECTORY, "images")

plt.style.use('seaborn')
plt.rcParams['font.size'] = 16.0
sns.set_style('dark')

if __name__ == "__main__":
    path = f"{DATA_DIRECTORY}/spark_data.csv"
    efficiency = pd.read_csv(path)
    efficiency.columns = ['samples', 'filter', 'count', 'old', 'new', 'groupby']

    ##
    fig, axes = plt.subplots(figsize=(8,8))
    fig.suptitle("Code improvements")
    colors = ['b', 'g']
    for i, column in enumerate(['old', 'new']):
        x = efficiency['samples'].values
        axes.plot(x, efficiency[column].values, color=colors[i], label = f"{column.capitalize()} function")
    axes.legend(loc='best')
    axes.set_xscale('log')
    axes.set_xlabel("Samples (log)")
    axes.set_ylabel("Execution time (s)")
    plt.savefig(f"{IMAGE_DIRECTORY}/functions.png", dpi=120)

    ##
    plots = ['filter', 'groupby', 'count']
    fig, axes = plt.subplots(len(plots), 1, figsize=(8,8))
    #fig.suptitle("Spark function execution times")
    for i, column in enumerate(plots):
        ax = axes[i]
        x = efficiency.samples.values
        y = efficiency[column].values
        ax.plot(x, y, color='b')
        ax.set_xscale('log')
        ax.set_title(f".{column}()")
    fig.tight_layout(pad=1)
    plt.savefig(f"{IMAGE_DIRECTORY}/comparison.png", dpi=120)

    ##
    fig, axes = plt.subplots(len(plots), 1, figsize=(8,8))
    for i, column in enumerate(plots):
        ax = axes[i]
        x = efficiency.samples.values
        y = efficiency[column].values
        ax.plot(x[1:], y[1:], color='b')
        ax.set_xscale('log')
        ax.set_title(f".{column}()")
    fig.tight_layout(pad=1)
    plt.savefig(f"{IMAGE_DIRECTORY}/comparison2.png", dpi=120)