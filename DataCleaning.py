from datetime import datetime
import pandas as pd

def CreateGroupHourWise(k, n):
       
    if(k<10):
        dir="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/2022/0"+str(k)+"/"
        if(n<10):
             path=dir+"2022-0"+str(k)+"-0"+str(n)+"-prices.csv"
        elif(n>=10):
             path=dir+"2022-0"+str(k)+"-"+str(n)+"-prices.csv"
    elif(k>=10):
        dir="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/2022/"+str(k)+"/"
        if(n<10):
             path=dir+"2022-"+str(k)+"-0"+str(n)+"-prices.csv"
        elif(n>=10):
             path=dir+"2022-"+str(k)+"-"+str(n)+"-prices.csv"

    
    print(path)  
    day=pd.read_csv(path)
    day.drop('station_uuid', axis=1, inplace=True)
    day.head(5)
    day['date']=pd.to_datetime(day['date'], utc=True)
    day['hour']=pd.to_datetime(day['date']).dt.hour
    day['date']=pd.to_datetime(day['date']).dt.date
    day1_group=day.groupby(['hour'])[["diesel","e5","e10"]].agg({'mean'})
    day1_group=day1_group.round(3)
    day1_group=day1_group.reset_index()
 
        
    if(k<10):
        dir="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/2022/0"+str(k)+"/"
        if(n<10):
            writepath=dir+"latest_2022-0"+str(k)+"-0"+str(n)+"-prices.csv"
        elif(n>=10):
            writepath=dir+"latest_2022-0"+str(k)+"-"+str(n)+"-prices.csv"
    elif(k>=10):
        dir="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/2022/"+str(k)+"/"
        if(n<10):
            writepath=dir+"latest_2022-"+str(k)+"-0"+str(n)+"-prices.csv"
            
        elif(n>=10):
            writepath=dir+"latest_2022-"+str(k)+"-"+str(n)+"-prices.csv"
            

   
    print('writepath', writepath)
    day1_group.to_csv(writepath)
    


    

def convertDatetime(hour, **kwargs):  
    date_string="2022-"  
    month= int(kwargs['m'])
    day=int(kwargs['d'])
   
    if(month <10):
        m="0"+str(month)
    elif(month>=10):
        m=str(month)
    if(day<10):
        d="0"+str(day)
    elif(day>=10):
        d=str(day)
    h=int(hour)       
    if(h<10):
        h="0"+str(h)
    elif(h>=10):
        h=str(h)
            
            
    date_string=date_string+m+"-"+d+" "+h+":00:00"
    print(date_string)
    
    return date_string



def convertToDateTime(k,n):
    
    if(k<10):
        dir="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/2022/0"+str(k)+"/"
        if(n<10):
             readpath=dir+"latest_2022-0"+str(k)+"-0"+str(n)+"-prices.csv"
        elif(n>=10):
            readpath=dir+"latest_2022-0"+str(k)+"-"+str(n)+"-prices.csv"
    elif(k>=10):
        dir="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/2022/"+str(k)+"/"
        if(n<10):
             readpath=dir+"latest_2022-"+str(k)+"-0"+str(n)+"-prices.csv"
        elif(n>=10):
             readpath=dir+"latest_2022-"+str(k)+"-"+str(n)+"-prices.csv"
    print('readpath', readpath)
    data=pd.read_csv(readpath)
    data.dropna( axis=0, inplace=True)
    data.head(5)
    m=k
    d=n
    
   
    data['datetime']=data['hour'].apply(convertDatetime,  m=k, d=n)
    
    data=data[['datetime','hour', 'diesel', 'e5', 'e10']]
    data.to_csv(readpath)


def consolidateMonths(k, i, finaldataframe):
        if(k<10):
            dir="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/2022/0"+str(k)+"/"
            if(i<10):
                readpath=dir+"latest_2022-0"+str(k)+"-0"+str(i)+"-prices.csv"
            elif(i>=10):
                readpath=dir+"latest_2022-0"+str(k)+"-"+str(i)+"-prices.csv"
        elif(k>=10):
            dir="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/2022/"+str(k)+"/"
            if(i<10):
                readpath=dir+"latest_2022-"+str(k)+"-0"+str(i)+"-prices.csv"
            elif(i>=10):
                readpath=dir+"latest_2022-"+str(k)+"-"+str(i)+"-prices.csv"
            print('readpath', readpath)
        filewisedata=pd.read_csv(readpath)
        filewisedata=filewisedata[['datetime', 'hour','diesel','e5', 'e10']]
        finaldataframe=pd.concat([finaldataframe,filewisedata])
        print("size0", finaldataframe.size)
      
        return finaldataframe
    
def consolidateYears(finaldataframe):
    
    for year in range(2019, 2023):
        
        readpath="C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/"+str(year)+"/"+str(year)+".csv"
        filewisedata=pd.read_csv(readpath)
        filewisedata=filewisedata[['datetime', 'hour','diesel','e5', 'e10']]
        finaldataframe=pd.concat([finaldataframe,filewisedata])
        print("year size", finaldataframe.size)
    
    return finaldataframe



finaldataframe=pd.DataFrame(columns=['datetime', 'hour','diesel','e5', 'e10'])

for k in range(1, 3):
    
    low=0
    if k==2:
        low=7
    if (k==3) | (k==4) | (k==6)|(k==8) |(k==9)  | (k==10) | (k==11):
        low=31
    elif (k==1)  |  (k==5) | (k==7)  | (k==12):
        low=32
        
    for i in range(1,low):
        print(i)
       # CreateGroupHourWise(k,i)
        #convertToDateTime(k, i)
        #finaldataframe=consolidateMonths(k,i,finaldataframe)
        
        print(finaldataframe.tail(5))  
    allyearsdataframe=consolidateYears(finaldataframe)      
    allyearsdataframe.to_csv("C:/Users/Dell/Documents/SRH_Course/Semister-2/DataStoryTelling2/Projects/FinalProject/"+"finaldataset.csv")
        
        
    
        
   