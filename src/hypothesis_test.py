import os
import argparse
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as stats
from tabulate import tabulate

plt.style.use('seaborn')
plt.rcParams['font.size'] = 16.0

SCRIPT_DIRECTORY = os.path.realpath("")
HOME_DIRECTORY = os.path.split(SCRIPT_DIRECTORY)[0]
DATA_DIRECTORY = os.path.join(HOME_DIRECTORY, "data")
IMAGE_DIRECTORY = os.path.join(HOME_DIRECTORY, "images")

if __name__ == "__main__":
    ## argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', default="bootstrap", \
                         type=str, help='Bootstrap individual samples from country means (bootstrap) \
                         or use the country-level means only (mean). Default : bootstrap')
    args = vars(parser.parse_args())
    if args['mode'] == "mean":
        do_bootstrap = False
    else:
        do_bootstrap = True

    path = f"{DATA_DIRECTORY}/country_preferences.csv"
    by_country = pd.read_csv(path)
    cluster = pd.read_csv(f"{DATA_DIRECTORY}/country_cluster_map.csv")

    cluster = cluster[['ISO3', 'Cluster']].rename(columns={"Cluster":"cluster"})
    cluster_stats = pd.DataFrame({'cluster':[0, 1, 2], 'cluster_name': ['West', 'East', 'South'],\
                             'color':["r", "g", "b"] })

    by_country = cluster.merge(by_country, on="ISO3")
    by_country.cluster = by_country.cluster.astype(int)

    factor = "gender"

    by_country = by_country.where(by_country[f'n_{factor}'] != 0).dropna()

    fig, axes = plt.subplots(3, 1, figsize=(8,9))
    fig2, ax2 = plt.subplots()

    values = np.array([])
    file_suffix = ""
    ## plotting
    for cluster in [0,1,2]:
        group = by_country[by_country["cluster"] == cluster]
        means = np.array(group[f"p_{factor}"])
        total_responses = int(np.sum(group[f"n_{factor}"]))
        country_weights = np.array(group[f"n_{factor}"] / total_responses)

        if do_bootstrap:
            values = np.random.choice(means, size=total_responses, p=country_weights)
            file_suffix = "booted"
        else:
            values = means

        ax = axes[cluster]
        x = np.linspace(np.min(values) - .1, np.max(values) + .1, 100)
        ax.set_xlim(np.min(x) - .1, np.max(x) + .1)
        ax.set_xlabel(f"Probability of choosing default {factor}")

        ax.hist(values, density=True, label=f"density (n = {len(means)})")
        mu, std = np.mean(values), np.std(values)

        ax.plot(x, stats.norm.pdf(x, loc=mu, scale=std), 'r-', alpha=0.8,\
                label=f"normal distribution")
        ax.set_yticklabels([])
        ax.set_title(f"{cluster_stats.cluster_name[cluster]}")
        ax.legend(loc='best')

        fig.tight_layout(pad=1)
        fig.savefig(f"{IMAGE_DIRECTORY}/{factor}_hists{file_suffix}.png", dpi=120)

        x = np.linspace(0, 1, 100)
        ax2.plot(x, stats.norm.pdf(x, loc=mu, scale=std), f'{cluster_stats.color[cluster]}-', alpha=0.8,\
                label=f"{cluster_stats.cluster_name[cluster]} (mean = {round(mu, 3)})")
        ax2.set_xlim(stats.norm.ppf(.001, loc=mu, scale=std), stats.norm.ppf(.999, loc=mu, scale=std))
        ax2.axvline(x=mu, linestyle="--", color='k')
        ax2.set_yticklabels([])
        ax2.set_title(f"{factor.capitalize()} by cluster")
        ax2.legend(loc='best')

        fig2.tight_layout(pad=1)
        fig2.savefig(f"{IMAGE_DIRECTORY}/{factor}_distributions{file_suffix}.png", dpi=120)

    ## retrieving p values
    cluster_compared_p = np.zeros((3,3))

    values, valuesnext = np.array([]), np.array([])
    for cluster, nextcluster in zip([0, 1, 2], [1, 2, 0]):
        b = (cluster + 1) % 3

        group = by_country[by_country["cluster"] == cluster]
        groupnext = by_country[by_country["cluster"] == nextcluster]
        means = np.array(group[f"p_{factor}"])
        meansnext = np.array(groupnext[f"p_{factor}"])
        total_responses = int(np.sum(group[f"n_{factor}"]))
        total_responsesnext = int(np.sum(groupnext[f"n_{factor}"]))
        country_weights = np.array(group[f"n_{factor}"] / total_responses)
        country_weightsnext = np.array(groupnext[f"n_{factor}"] / total_responsesnext)

        if do_bootstrap:
            values = np.random.choice(means, size=total_responses, p=country_weights)
            valuesnext = np.random.choice(meansnext, size=total_responsesnext, p=country_weightsnext)
            file_suffix = "booted"
        else:
            values, valuesnext = means, meansnext

        
        #print(f"Variances : ({cluster} : {np.var(values)}), ({nextcluster} : {np.var(valuesnext)}) ")
        t, p = stats.ttest_ind(values, valuesnext, equal_var=False)
        cluster_compared_p[cluster][nextcluster] = p
        cluster_compared_p[nextcluster][cluster] = p
            
    significance_matrix = pd.DataFrame(data=cluster_compared_p, \
                                    index=['West', 'East', 'South'], \
                                    columns=['West', 'East', 'South'])
    table = tabulate(significance_matrix, headers=significance_matrix.columns, tablefmt='github')

    tablefile = open(f"../data/table{file_suffix}.txt", "w+")
    for line in table:
        tablefile.write(line)
    