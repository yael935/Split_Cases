import pandas as pd
from datetime import datetime, timedelta


# ####################### INPUTS ######################## #
# Please fill the inputs for the split cases detection method
logName = 'BPI_2019_3_way_after.csv'  # csv file of the log
caseID = 'Case ID'  # Case ID column name
timeStamp = 'Complete Timestamp'  # timestamp column name
resource = 'Resource'  # resource column name
firstTimeStamp = pd.to_datetime('2018-01-01 00:00:00', format='%Y-%m-%d %H:%M:%S')  # timestamp from which we start
lastTimeStamp = pd.to_datetime('2019-01-16 16:10:00', format='%Y-%m-%d %H:%M:%S')  # timestamp in which we end
splitColumn = 'Cumulative net worth (EUR)'  # split data item (price, amount...)
thresholdValue = 100000  # threshold value of the data item condition
unitingAttribute = '(case) Vendor'  # uniting attribute (customer, vendor...). split cases have the same values for this column
x = 0.5  # ratio of time-frame size and mean time between cases. TimeFrameSize = x * timeBetweenCases
y = 0.1  # ratio of iteration size and mean time between cases. Iteration = y * timeBetweenCases


# FUNCTION: GET GROUPS OF SUSPECTS
# return groups which refer to the same uniting attribute
def get_groups_of_suspects(func_log):
    groups = []
    dup_values = func_log[func_log.duplicated([unitingAttribute])]
    dup_values = dup_values[unitingAttribute].unique()  # values of 'uniting attribute' that appear more than once
    for j in range(len(dup_values)):  # create groups of suspects - cases with same value in uniting attribute
        func_mask = func_log[unitingAttribute] == dup_values[j]
        func_log_masked = func_log.loc[func_mask]
        group = [func_log_masked[caseID]]
        groups.append(group)
    return groups


# FUNCTION: get splitColumn value of a case
def get_split_column_value(case_id):
    case_tuple = log[log[caseID] == case_id]
    if case_tuple.empty:
        return 0
    else:
        return case_tuple.iloc[0][splitColumn]


# FUNCTION: get list of lists (a list of all suspects). check if each group is indeed split according to threshold
def is_split_cases(cases):
    aggregate_split_column = 0
    for h in range(len(cases)):
        aggregate_split_column += get_split_column_value(cases[h])
    if aggregate_split_column > thresholdValue:
        return True
    else:
        return False


# FUNCTION: get array of all suspects. return array of groups that cross the threshold
def get_split(suspects_series):
    groups = []
    for k in range(len(suspects_series)):  # for each group of suspects
        # set an array of the suspects in the group
        suspects = []
        temp_array = str(suspects_series[k][0]).split()
        for m in range(len(temp_array)-4):
            if m % 2 == 1:
                suspects.append(temp_array[m])
        if is_split_cases(suspects):
            groups.append(suspects)
    return groups


# current timestamp when start running, to measure execution time
runStart = datetime.now()

# Read the log, and data-fix timestamps
log = pd.read_csv(logName, sep=',', header=0)
log[caseID] = log[caseID].astype(str)
log[timeStamp] = log[timeStamp].str.slice(0, 19)  # slice timestamp to required format
log[timeStamp] = pd.to_datetime(log[timeStamp], format='%Y/%m/%d %H:%M:%S')
log = log.sort_values(timeStamp)

# Filter log so that only first event of each case remains
conditionLog = log
conditionLog = log.groupby(caseID).first().reset_index()

# calculate mean time between cases
sum_diff = timedelta(minutes=0)
for q in range(len(conditionLog)-1):
    sum_diff += (conditionLog.iloc[q+1][timeStamp]-conditionLog.iloc[q][timeStamp])
timeBetweenCases = (sum_diff/(len(conditionLog)-1)).total_seconds()/60

# Variables: timeframe_size and iteration_size
timeFrameSize = timedelta(minutes=x*timeBetweenCases)  # amount of time being examined each iteration
iteration = timedelta(minutes=y*timeBetweenCases)  # how much we move the time-frame each iteration
print('timeFrame size: ', timeFrameSize)
print('iteration size: ', iteration, '\n')

# Filter log so that it only contains cases that have a preferable path.
# The preferable path is indicated by splitColumn<thresholdValue.
mask2 = (conditionLog[splitColumn] < thresholdValue)
conditionLog = conditionLog.loc[mask2]

# Prepare initial values for time-frame filtering
frameStart = firstTimeStamp
frameEnd = frameStart + timeFrameSize
isFinished = True  # checks if all time frames have been checked
allSuspects = []

# Time-frame loop
while isFinished:
    mask = (conditionLog[timeStamp] >= frameStart) & (conditionLog[timeStamp] <= frameEnd)
    timeFrameLog = conditionLog.loc[mask]
    # Loop over each resource
    resources = timeFrameLog[resource].unique()
    for i in range(len(resources)):
        mask = timeFrameLog[resource] == resources[i]
        resourceTimeFrameLog = timeFrameLog.loc[mask]
        function_log = resourceTimeFrameLog
        allSuspects += get_groups_of_suspects(function_log)
    frameStart += iteration
    frameEnd = frameStart + timeFrameSize
    isFinished = frameEnd < lastTimeStamp

suspectSplitCasesNotUnique = get_split(allSuspects)

# make the list of lists unique
suspectSplitCases = []
for elem in suspectSplitCasesNotUnique:
    if elem not in suspectSplitCases:
        suspectSplitCases.append(elem)

# current timestamp when end running, to measure runtime duration
runEnd = datetime.now()
runtimeDuration = runEnd - runStart

# print results
print('Runtime Duration: ', runtimeDuration, '\n\n')
print('~~~~~~~~~ IDENTIFIED SPLIT CASES ~~~~~~~~~ \n\n   Length: ', len(suspectSplitCases), '\n\n', suspectSplitCases)