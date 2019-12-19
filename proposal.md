## Capstone I : Moral distance 
### Paxton Hyde

#### Motivation
I am interested in looking at the "moral distance" between different cultures. [This article](https://www.nature.com/articles/s41586-018-0637-6.epdf?referrer_access_token=5SBKjXqSe9W89TIoohZIvNRgN0jAjWel9jnR3ZoTv0OR8PKa5Kws8ZzsJ9c7-2Qpul1Vc1F8wY0eIbuOUfmConm9MpvB9JNjnmyrCoj2uOCRbTFI3tmUdV2tYqE2L6ifmrb-tsgAoOc9lINEcKDSOkEkmhLSjqz8bf1ACffMhu6EiQ2ZXU5cHbrFXuiJoXRMxuojb8tUZNFuN2R4kksBNzsaFxxkByF7rx-cxTgMCGvimdjBOY0vMtRkwpXvk9EyI0NunRjTj6Bi1No-Hv00gQBUqxE6xdxW_2lzO7zwdeMnyED_zlEwNHFqcd9GAeuWl-CtPy9UtgwYO_5VKTLt50rGC5vG2pcPQsAXVtbF58CCLdPZcJHGJit56_0t8-lq0fjzKjPGd6HBGyxlP6-5HpLh6sV0tO9TjZmLkIKUVFKNZPjEb8N5_Ysqk0IbycCC&tracking_referrer=www.technologyreview.com "The Moral Machine Experiment") published in Nature analyzed user responses to an MIT online survery called the Moral Machine. The survey is a series of choices between two scenarios where a self-driving car is going to crash, and the user must choose which scenario is (morally) preferable. In this way the survey collects data about preferences for intervention (swerving to avoid a crash versus staying on the same course), protecting passengers over pedestrians, and protecting different types of characters based on their age, gender, fitness, societal value (doctors versus criminals), or whether the character was breaking the law (by jaywalking).

![A scenario][choice]

Based on the survey responses, the authors grouped the countries with responses into three categories (Western, Eastern, and Southern) based on their moral preferences.

#### Data
The authors of the paper made their data publicly available. The bulk of the dataset is a list of survey responses with description of the two situations and the user's choice. Some of the column labels are vague as to what exactly they are describing, although after playing the game I made some progress. Some entries also include user demographic information: age, country, income, educational attainment, and a self assessment of political stance and religiosity on a sliding scale.

The dataset also includes other tables documenting their results, particularly the moral distances between cultures that they had calculated based on the survey data alone.


#### MVP

I am somewhat concerned that the data is much too clean and the significant results are already parsed. My solution will be to use a Bayesian approach to evaluate the validity of the general moral categories proposed in the article. It seems naive to base an evaluation of moral preferences solely on the results of an internet survey.

To do this I will look at other data that I could use to categorize countries by moral preference, for example, a corruption index, a democracy index, a freedom of press index, or a 'societal openness' index.

In any case my minimum viable product is some thorough statistical analysis of the moral machine dataset, and my ideal product is a comparison of the Nature article findings with other moral preference data.



[choice]: https://raw.githubusercontent.com/paxtonhyde/capstone-I/master/images/Screenshot%202019-12-19%2007.19.56.png


