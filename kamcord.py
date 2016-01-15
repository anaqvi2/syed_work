import pandas as pd
import datetime

class kamcord:
    
    def table(self, start_date, end_date, day=7):
    
        temp = pd.read_csv('data.csv', iterator=True, chunksize=1000) 
        all_range=pd.concat([chunk[(chunk['event_time'] >= start_date) & (chunk['event_time'] < (datetime.datetime.strptime(end_date, "%Y-%m-%d")+datetime.timedelta(days=1)).strftime('%Y-%m-%d')) & (chunk['event_name']=="UI_OPEN_COUNT")] for chunk in temp])
        user_only= (all_range[['user_id']]).drop_duplicates()  
        all_range['event_time']=pd.to_datetime(all_range['event_time']).apply(lambda x: x.date()) 
        all_range=(all_range[["user_id", "os_name", "sdk_version", "event_time"]]).drop_duplicates() 
        
        temp = pd.read_csv('data.csv', iterator=True, chunksize=1000)
        self.all=pd.DataFrame(pd.concat([chunk[chunk['user_id'].isin(user_only['user_id']) & (chunk['event_time'] < (datetime.datetime.strptime(end_date, "%Y-%m-%d")+datetime.timedelta(days=1)).strftime('%Y-%m-%d')) & (chunk['event_name']=="UI_OPEN_COUNT")] for chunk in temp]).groupby("user_id").aggregate(min)['event_time']).reset_index()
        self.all['start_date']=pd.to_datetime(self.all['event_time']).apply(lambda x: x.date())
        self.all=pd.merge(self.all, all_range, how='inner', left_on=["user_id", "start_date"], right_on=["user_id", "event_time"])

        temp = pd.read_csv('data.csv', iterator=True, chunksize=1000) # Same as above        
        after_day_metric=pd.concat([chunk[(chunk['event_time'] >= (datetime.datetime.strptime(start_date, "%Y-%m-%d")+datetime.timedelta(days=day)).strftime('%Y-%m-%d')) & (chunk['event_time'] < (datetime.datetime.strptime(end_date, "%Y-%m-%d")+datetime.timedelta(days=(day+1))).strftime('%Y-%m-%d')) & (chunk['event_name']=="UI_OPEN_COUNT") & (chunk['user_id'].isin(self.all['user_id']))] for chunk in temp])
        after_day_metric['specified_days']=((pd.to_datetime(after_day_metric['event_time']).apply(lambda x: x.date()))+ datetime.timedelta(days=-day))
        self.complete = pd.merge(((after_day_metric[["user_id", "os_name", "sdk_version", "specified_days"]]).drop_duplicates()), (self.all[["user_id", "start_date"]]), how='inner', left_on=['user_id', 'specified_days'], right_on=['user_id', 'start_date'])
        self.days=day
        
    def analysis(self, **kwargs):
        if 'sdk' in kwargs: 
            final=self.complete[(self.complete['sdk_version']).isin(kwargs['sdk'])]
            total=self.all[(self.all['sdk_version']).isin(kwargs['sdk'])]
        if 'os' in kwargs: 
            final=self.complete[(self.complete['os_name']).isin(kwargs['os'])]
            total=self.all[(self.all['os_name']).isin(kwargs['os'])]
            if 'sdk' in kwargs:
                final=final[(final['sdk_version']).isin(kwargs['sdk'])]
                total=total[(total['sdk_version']).isin(kwargs['sdk'])]
        elif len(kwargs)==0: # if none are provided
            final=self.complete
            total=self.all
        print ("The SDK's %d Day UI Retention is %.2f%%" % (self.days, (float((len(final.axes[0])) / float(len(total.axes[0])))*100)))

# answer=kamcord()

# Question 1: 
# answer.table('2014-09-01', '2014-09-30')
# answer.analysis()
# ANS: The SDK's 7 Day UI Retention is 17.96%

# Question 2: 
# answer.table('2014-09-08', '2014-09-10')
# answer.analysis(os=["android"])
# ANS: The SDK's 7 Day UI Retention is 3.19%

# Question 3: 
# answer.table('2014-09-01', '2014-09-30')
# answer.analysis(os=["IOS"], sdk=["1.7.5"]) 
# ANS: The SDK's 7 Day UI Retention is 31.00%

### Syed Testing ####

# answer.table('2014-09-01', '2014-09-30')
# answer.analysis(sdk=["1.7.0", "1.4.4"])
# The SDK's 7 Day UI Retention is 13.35%

# answer.table('2014-09-01', '2014-09-30')
# answer.analysis(os=["IOS", "android"],sdk=["1.7.5", "1.4.4"])
# The SDK's 7 Day UI Retention is 13.28%

# answer.table('2014-09-01', '2014-09-30', 10)
# answer.analysis()
# The SDK's 10 Day UI Retention is 17.19%

# answer.table('2014-09-01', '2014-09-30', 10)
# answer.analysis(os=["IOS", "android"],sdk=["1.7.5", "1.4.4"])
# The SDK's 10 Day UI Retention is 12.53%
