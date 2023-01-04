def weekend_or_not(day):
    if(day=='Saturday' or day=='Sunday'):
        day='Weekend'
    else:
        day='Weekday'
    return day

def get_season(date):
    day=int(date.strftime("%d"))
    month=int(date.month)
    if ((month==3 and day>= 20) or (month>3 and month<6) or (month ==6 and day<22) ):
        res="Spring"
    elif ((month==6 and day>= 22) or (month>6 and month<9) or (month ==9 and day<23) ):
        res="Summer"
    elif  ((month==9 and day>= 23) or (month>9 and month<12) or (month ==12 and day<21) ):
        res="Autumn"
    else :
        res="Winter"
    return res

def add_weekend_column(df):
    df['week_end'] = df['day_of_week'].apply(weekend_or_not).replace(['Weekday', 'Weekend'], [0, 1])

def add_season_column(df):
    df['season'] = df['date'].apply(get_season)

def augment_df(df):
    add_weekend_column(df)
    add_season_column(df)
