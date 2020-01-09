import pyspark as ps
import pyspark.sql.functions as f

attributes = {"Intervention" : ["Saved = 0 AND Intervention = 0", "Saved = 1 AND Intervention = 1"],\
        "Legality" : ["Saved = 1 AND CrossingSignal = 1", "Saved = 0 AND CrossingSignal = 2"],\
        "Utilitarian" : ['More', 'Less'],\
        "Gender" : ['Male', 'Female'],\
        "Social Status" : ['High', 'Low'],\
        "Age" : ['Young', 'Old']}

## this one is universal has less .count() calls than p_factor so should be slightly faster
def p_factor2(dataframe, attribute):
    '''
    Returns the proportion of choices probability and sample_size from spark DataFrame dataf 
    that favored the default choice for attribute.
        Parameters: dataf (Spark DataFrame), attribute (str)
        Returns: tuple: (p (float), n (int))
    '''
    ## retrieve the conditions describing the attribute
    try:
        default, nondefault = attributes[attribute]
    except KeyError:
        print("p_factor received an invalid attribute argument.")
        return None  

    ##
    if attribute == "Legality":
        sample = dataframe.filter("CrossingSignal != 0 AND PedPed = 1")
        ## above line credit Edmond Awad, MMFunctionsShared.R
        ## found at: https://osf.io/3hvt2/files/
        positive = dataframe.filter(default or nondefault)

    elif attribute == "Intervention":
        positive = dataframe.filter("Saved = Intervention")

    else: ## if attribute is one of ["Utilitarian", "Gender", "Social Status", "Age"]
        sample = dataframe.filter(f"ScenarioType = '{attribute}' ")

        default_choice = f"Saved = 1 AND AttributeLevel = '{default}'"
        nonnondefault_choice = f"Saved = 0 AND AttributeLevel = '{nondefault}'"
        positive = dataframe.filter(default_choice or nonnondefault_choice)

    ##
    sample_size = sample.count()
    try:
        probability = positive.count() / sample_size
    except ZeroDivisionError:
        probability = -1
    
    return (probability, sample_size)

def p_factor(dataf, attribute):
    '''
    Returns the proportion of choices from data in dataf that favored the default choice, 
    default (str) the default choice for the factor, nondefault (str) the alternative choice
    for the factor, and n (int) the number of choices analyzed with the factor corresponding
    to the dimension.
        Parameters: dataf (Spark Dataframe), attribute (str)
        Returns: tuple: p (float), default (str), nondefault (str), 
    '''
    if attribute not in ["Utilitarian", "Gender", "Social Status", "Age"]:
        print("p_factor received an invalid attribute.")
        return None
    default, nondefault = attributes[attribute]
    
    factor = dataf.filter(f"ScenarioType = '{attribute}' ")
    n = factor.count()
    # probability of having chosen the default
    defs = factor.filter(f"Saved = 1 AND AttributeLevel = '{default}'").count()
    # probability of having not chosen the nondefault
    nonnondefs = factor.filter(f"Saved = 0 AND AttributeLevel = '{nondefault}'").count()
    try:
        return ( round((defs + nonnondefs) / n, 4), n, default, nondefault )
    except ZeroDivisionError:
        #print("p_factor received a dataframe without revelant entries.")
        return None

def p_intervention(dataf):
    '''
    Returns the proportion of choices in dataf that favored
    intervention over non-intervention, and n the number of choices analyzed.
        Params: dataf (Spark Dataframe)
        Returns: p (float), n (int)
    '''
    # probability of having chosen commission
    commits = dataf.filter("Saved = 1 AND Intervention = 1").count()
    # probability of having not chosen omission, meaning that the user must have chosen
    # commission in the scenario
    omits = dataf.filter("Saved = 0 AND Intervention = 0").count()
    n = dataf.count()
    try:
        return (round((commits + omits) / n, 4), n)
    except ZeroDivisionError:
        #print("p_intervention received a dataframe without revelant entries.")
        return None

def p_legality(dataf):
    '''
    Returns p the proportion of choices from data in dataf that favored saving pedestrians
    crossing legally, and n the number of choices analyzed with a legal dimension.
        Parameters: dataf (Spark Dataframe)
        Returns: tuple: (p (float), n (int))
    '''
    legality = dataf.filter("CrossingSignal != 0 AND PedPed = 1")
    ## above line credit Edmond Awad, MMFunctionsShared.R
    ## found at: https://osf.io/3hvt2/files/
    n = legality.count()
    
    # probability of having chosen to save law-abiding
    peds = legality.filter("Saved = 1 AND CrossingSignal = 1").count()
    # probability of having chosen to not save non-law-abiding
    jwalkers = legality.filter("Saved = 0 AND CrossingSignal = 2").count()
    
    try:
        return (round((peds + jwalkers) / n, 4), n)
    except ZeroDivisionError:
        #print("p_legality received a dataframe without revelant entries.")
        return None