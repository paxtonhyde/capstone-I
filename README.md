# compelling title
#### Capstone I for Galvanize Data Science Immersive

## Question

## Data
The data were collected through [the Moral Machine](http://moralmachine.mit.edu/), an online survey game created by MIT artificial intelligence researchers. Users decide the morally preferable action for an autonomous vehicle from two scenarios presented by the game. A game session lasts 13 rounds, after which the user has the option to provide demographic information including their age, gender, annual income, country, and political and religious orientation on a sliding scale. 

#### Description of tables
*SharedResponses.csv* – Unique response identification, session, user, and detailed scenario data.

*SharedResponsesFullFirstSessions.csv* – ResponseID,ExtendedSessionID,UserID,ScenarioOrder,Intervention,PedPed,Barrier,CrossingSignal,AttributeLevel,ScenarioTypeStrict,ScenarioType,DefaultChoice,NonDefaultChoice,DefaultChoiceIsOmission,NumberOfCharacters,DiffNumberOFCharacters,Saved,Template,DescriptionShown,LeftHand,UserCountry3

*SharedResponsesSurvey.csv* – ResponseID,ExtendedSessionID,UserID,ScenarioOrder,Intervention,PedPed,Barrier,CrossingSignal,AttributeLevel,ScenarioTypeStrict,ScenarioType,DefaultChoice,NonDefaultChoice,DefaultChoiceIsOmission,NumberOfCharacters,DiffNumberOFCharacters,Saved,Template,DescriptionShown,LeftHand,UserCountry3,Review_age,Review_education,Review_gender,Review_income,Review_political,Review_religious

countryInfo.csv – Country-specific information, particularly country ISO code, continent, languages, population, and area.
country_cluster_map.csv – Country ISO code, name, and cluster (0, 1, or 2, corresponding to East, West, and South).
cultures.csv – Country name and general category culture from: Catholic, Protestant, English, Islamic, Orthodox, Confucian, Baltic, LatinAmerica, SouthAsia, or none.
moral_distance.csv – Country name, distance metric (USA = 0, max ~6 (Brunei))
CountriesChangePr.csv – By-country estimate and standard error for moral preferences including omission/commission, passengers/pedestrians, gender, fitness, social status, age, legality, and species. 

One line of the data

#### Descriptions of columns

## References
* MIT Technology Review [link to article](https://www.technologyreview.com/s/612341/a-global-ethics-study-aims-to-help-ai-solve-the-self-driving-trolley-problem/?utm_medium=tr_social&utm_campaign=site_visitor.unpaid.engagement&utm_source=Twitter#Echobox=1576511368 "Should a self-driving car kill the baby or the grandma? Depends on where you’re from.")
* Nature [link to paper](https://www.nature.com/articles/s41586-018-0637-6.epdf?referrer_access_token=5SBKjXqSe9W89TIoohZIvNRgN0jAjWel9jnR3ZoTv0OR8PKa5Kws8ZzsJ9c7-2Qpul1Vc1F8wY0eIbuOUfmConm9MpvB9JNjnmyrCoj2uOCRbTFI3tmUdV2tYqE2L6ifmrb-tsgAoOc9lINEcKDSOkEkmhLSjqz8bf1ACffMhu6EiQ2ZXU5cHbrFXuiJoXRMxuojb8tUZNFuN2R4kksBNzsaFxxkByF7rx-cxTgMCGvimdjBOY0vMtRkwpXvk9EyI0NunRjTj6Bi1No-Hv00gQBUqxE6xdxW_2lzO7zwdeMnyED_zlEwNHFqcd9GAeuWl-CtPy9UtgwYO_5VKTLt50rGC5vG2pcPQsAXVtbF58CCLdPZcJHGJit56_0t8-lq0fjzKjPGd6HBGyxlP6-5HpLh6sV0tO9TjZmLkIKUVFKNZPjEb8N5_Ysqk0IbycCC&tracking_referrer=www.technologyreview.com "The Moral Machine Experiment")

