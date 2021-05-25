import datetime
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import jieba
from pyecharts.charts import Bar,Line,Pie,Grid,Map

# 设置字体
matplotlib.rcParams['font.family']='sans-serif'
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False

# 任务一1.1 login预处理

login = pd.read_csv('./login.csv',encoding='gbk')
login.info()
# login中无缺失值

print(login.duplicated().value_counts())
# login中无重复值

def kill_repeated_time(col,df):
    ''' 将所有在一天以内的登录视为一次，以减小数据量'''
    df[col] = pd.to_datetime(df[col])
    df[col] = df[col].apply(lambda x:x.strftime('%y-%m-%d'))
    return df
login = kill_repeated_time('login_time',login)
login = login.drop_duplicates()
login.info()
# 将重复的数据删除

login['interval'] = pd.to_datetime('20-06-18',format='%y-%m-%d') - pd.to_datetime(login['login_time'],format='%y-%m-%d')
print(login.sample(10))
login = login.sort_values('interval',ascending=False)
print(login.head(15))
# 计算注册与现在的间隔值，并按间隔值降序排列，保存间隔值的降序

# 对login数据的地区细分
# 重建索引
login = login.reset_index()
for i in range(login.shape[0]):
    if login.loc[i,'login_place'][0:2]=='中国':
        login.loc[i,'国家']='中国'
        if '黑龙江' in login.loc[i,'login_place']:
             login.loc[i,'省份']='黑龙江'
             if len(login.loc[i,'login_place'])>5:
                 login.loc[i,'地区']=login.loc[i,'login_place'][5:]
             else:pass
        if '新疆维吾尔' in login.loc[i,'login_place']:
             login.loc[i,'省份']='新疆维吾尔'
             if len(login.loc[i,'login_place'])>7:
                 login.loc[i,'地区']=login.loc[i,'login_place'][7:]
             else:pass
        if '内蒙古' in login.loc[i,'login_place']:
             login.loc[i,'省份']='内蒙古'
             if len(login.loc[i,'login_place'])>5:
                 login.loc[i,'地区']=login.loc[i,'login_place'][5:]
             else:pass
        else:
             login.loc[i,'省份']=login.loc[i,'login_place'][2:4]
             login.loc[i,'地区']=login.loc[i,'login_place'][4:]
    else:
         li=[word for word in jieba.cut(login.iloc[i,2])]
         if len(li)==2:
             login.loc[i,'国家']=li[0]
             login.loc[i,'省份']=li[1]
         else:
             login.loc[i,'国家']=li[0]
    if i%10000==0:
         print(f'{round(i*100/(int(login.shape[0])),2)}%')

# 储存为excel
login.to_excel('预处理.xlsx')

# 任务一1.2 stud_info预处理

stud_info = pd.read_csv('./study_information.csv',encoding='gbk')
stud_info = stud_info.drop_duplicates()
stud_info.info()
# 不存在完全重复，价格存在空值

stud_info_price_null = stud_info[stud_info['price'].isnull()]
print(stud_info_price_null['course_id'].value_counts())
print(stud_info_price_null.info())
# 空值是课程51和96

stud_info_sample = stud_info.groupby('course_id').agg({'price':['max','min']})
print(stud_info_sample[(stud_info_sample['price']['max'] - stud_info_sample['price']['min']) != 0])
stud_info = stud_info.dropna()
stud_info.info()
print(stud_info.sample(5))
# 清除空值，不存在差异定价，暂不处理价格空缺

stud_info['learn_process'] = stud_info['learn_process'].apply(lambda x:int(x.split('width: ')[-1].split('%')[0]))
print(stud_info.sample(5))

# 任务一1.3 users预处理

# 缺省值处理
users = pd.read_csv('./users.csv',encoding='gbk')
users.info()
# user_id,school 存在空值

# 处理user_id 的空值
users_null = users[users['user_id'].isnull()]
users_null.info()
# user_id缺失较少，考虑直接删去
users.dropna(inplace=True,subset=['user_id'])
users.info()

# 处理school的空值
# 其大范围空缺，所以考虑创建0,1分类变量。
users.reset_index(inplace=True)
for i in range(users.shape[0]):
    if users.loc[i,'school'] is not np.nan:
        users.loc[i,'having school?'] = 1
    else:
        users.loc[i, 'having school?'] = 0
users.info()
print(users.sample(10))

# 处理recently_logged中的’--‘异常值

# 考虑登录后未下线
keys = login.groupby('user_id').login_time.max().index.tolist()
values = login.groupby('user_id').login_time.max().values.tolist()
login_time = {}
for i in range(len(keys)):
    login_time[keys[i]] = values[i]
u_2 = users[users.recently_logged != '--']
u_1 = users[users.recently_logged == '--']
for i in range(u_1.shape[0]):
    if u_1.iloc[i, 1] in login_time.keys():
        u_1.iloc[i, 3] = pd.to_datetime(login_time[u_1.iloc[i, 1]])
    else:
        if pd.to_datetime(u_1.iloc[i, 2]) + datetime.timedelta(days=int(u_1.iloc[i, 6]) / 480) > pd.to_datetime(
                '2020-06-18'):
            u_1.iloc[i,3] = pd.to_datetime('2020-06-18')
            print('修改时间为最新时间')
        else:
            u_1.iloc[i, 3] = pd.to_datetime(u_1.iloc[i, 2]) + datetime.timedelta(days=int(u_1.iloc[i, 6]) / 480)
users = pd.concat([u_1, u_2])
print(users.sample(10))

# 计算登录注册时间差
users['register_logged_time']=pd.to_datetime(users['recently_logged'])-pd.to_datetime(users['register_time'])
users['register_now_time']=pd.to_datetime('2020-06-18')-pd.to_datetime(users['register_time'])
users['logged_now_time']=pd.to_datetime('2020-06-18')-pd.to_datetime(users['recently_logged'])

# 计算正在上的课程数
users['classing'] = users['number_of_classes_join'] - users['number_of_classes_out']
users.info()

# 任务二 信息整合
# 每个人的选课数量
def nx_data(df=stud_info, group_name=['course_id', 'user_id']):
    # 得到共现字典
    user_dic = {}
    stu_info_data = df.groupby(group_name)['course_id'].count().unstack()
    column = stu_info_data.columns.tolist()
    for i in range(stu_info_data.shape[0]):
        user_dic[column[i]] = stu_info_data[stu_info_data[column[i]] == 1].index.tolist()

    # 构造共现矩阵
    course_name = list(set(df['course_id'].values.tolist()))
    course_data = pd.DataFrame(data=np.zeros(shape=(len(course_name), len(course_name))), index=course_name,
                               columns=course_name)
    for value in user_dic.values():
        if len(value) == 1:
            pass
        else:
            for i in range(len(value)):
                for j in range(i + 1, len(value)):
                    course_data.loc[value[i], value[j]] += 1
    return (user_dic, course_data)

user_dic, course_data = nx_data()
for i,key in enumerate(user_dic.keys()):
    users.loc[i,'选课数量']=len(user_dic[key])
print(users.head(10))

# 根据最近的地区合并
login_1=login.sort_values(by=['user_id','interval'])
login_del=login_1.user_id.drop_duplicates()
login_diff=login.iloc[list(login_del.index),:]
users_all=pd.merge(users,login_diff,on='user_id',how = 'left')
users_all.to_excel('整合信息.xlsx')
users_all=users_all.reset_index()
users_all=users_all.drop(columns=['index'])

# 任务三 可视化分析

# 国别分析
country = login[login['国家']!='中国'].国家.value_counts().index.tolist()
country_count = login[login['国家']!='中国'].国家.value_counts().values.tolist()
print(login.国家.value_counts())

# 折线图和玫瑰图
line=Line()
line.add( "",country,country_count,mark_point=["max", "min"],mark_line=["average"])
pie = Pie("", title_pos="55%")
pie.add( "", country, country_count, radius=[45, 65],center=[74, 50],legend_pos="80%",legend_orient="vertical",rosetype="radius",is_legend_show=False,is_label_show = True)

grid = Grid(width=900)
grid.add(line, grid_right="55%")
grid.add(pie, grid_left="50%")

# 中国省份分析（直方图）
provice=login[login['国家']=='中国'].省份.value_counts().index.tolist()
provice_count=login[login['国家']=='中国'].省份.value_counts().values.tolist()
bar=Bar()
bar.add('',provice,provice_count,mark_point=['max','min'],mark_line=['average'], is_datazoom_show=True)

# 玫瑰图百分比可视化
pie1 = Pie()
pie1.add(
        '',
        provice[:15],provice_count[:15],              #''：图例名（不使用图例）
        radius = [40,75],           #环形内外圆的半径
        is_label_show = True,       #是否显示标签
        legend_orient = 'bottom', #图例垂直
        rosetype="radius", #玫瑰饼图
        legend_pos = 'left'
        )

# 不同地域用户差异分析
print(users_all.groupby(['省份']).agg({'learn_time':['sum','mean','count'],'number_of_classes_now':['sum','mean'],'选课数量':['mean']}))

# 活跃用户可视化
line1=Line()
line1.add('',
         users_all[users_all['recently_logged']>'2020-01-01'].groupby(by='recently_logged').user_id.count().index.tolist(),
         users_all[users_all['recently_logged']>'2020-01-01'].groupby(by='recently_logged').user_id.count().values.tolist(),
        mark_point=['max'])

print(users_all[users_all['recently_logged']=='2020-06-11'].describe())
# 1/4注册时间到距离最新时间都是7天，或许是进行了推广活动

print(users_all[users_all['recently_logged']=='2020-06-11'])

#6月11日有一个异常点
# 当时进行学校注册优惠活动？
users_all[users_all['recently_logged']=='2020-06-11'].是否填写学校信息.value_counts()

# 区分工作日和休息日
for i in range(users_all.shape[0]):
    if i == 0:
        if datetime.is_workday(pd.to_datetime(users_all.iloc[i,2])):
            users_all.loc[0,'是否工作日']=1
        else:
            users_all.loc[0,'是否工作日']=0
    else:
        if datetime.is_workday(pd.to_datetime(users_all.iloc[i,2])):
            users_all.iloc[i,-1]=1
        else:
            users_all.iloc[i,-1]=0
print(users_all.是否工作日.value_counts())
users_all.groupby(['是否填写学校信息','是否工作日']).user_id.count()

# 用户流失度分析
ar=[str(aa)[:-14] for aa in users_all.groupby(['logged_now_time']).logged_now_time.count().index.tolist()]
kr=users_all.groupby(['logged_now_time']).logged_now_time.count().values.tolist()
line1=Line()
line1.add('',ar[:],kr[:],mark_line=["average"])

line2=Line()
line2.add('',ar[:140],kr[:140],mark_line=["average"])

line3=Line('')
line3.add('',ar[:30],kr[:30],mark_line=["average"])

line4=Line('')
line4.add('',ar[:10],kr[:10],mark_line=["average"])

grid = Grid()
grid.add(line1, grid_left="55%",grid_top='55%')
grid.add(line2, grid_right="55%",grid_top='55%')
grid.add(line3, grid_left="55%",grid_bottom='55%')
grid.add(line4, grid_right="55%",grid_bottom='55%')

print(users_all.groupby(['省份','logged_now_time']).user_id.count().unstack())
#对流失时间进行划分

for i in range(users_all.shape[0]):
    if int(str(users_all.loc[i,'logged_now_time'])[:-14]) > 150:
        users_all.loc[i,'流失时间划分']='大于150天'
    elif 90 <= int(str(users_all.loc[i,'logged_now_time'])[:-14]) < 150:
        users_all.loc[i,'流失时间划分']='大于90天'
    elif 30 <= int(str(users_all.loc[i,'logged_now_time'])[:-14]) < 90:
        users_all.loc[i,'流失时间划分']='大于30天'
    elif 15 <= int(str(users_all.loc[i,'logged_now_time'])[:-14]) < 30:
        users_all.loc[i,'流失时间划分']='大于15天'
    elif 7 <= int(str(users_all.loc[i,'logged_now_time'])[:-14]) < 15:
        users_all.loc[i,'流失时间划分']='大于7天'
    elif 0 <= int(str(users_all.loc[i,'logged_now_time'])[:-14]) < 7:
        users_all.loc[i,'流失时间划分']='7天内'

print(users_all.groupby(['省份','流失时间划分']).user_id.count().unstack())
print(users_all.groupby(['流失时间划分']).agg(['mean']).T)

# 用户课程选择分析
bar1=Bar()
bar1.add('',stud_info.course_id.value_counts().index.tolist()[:30],stud_info.course_id.value_counts().values.tolist()[:30]
         ,mark_line=['average']
         ,mark_point=['max','min']
         , is_datazoom_show=True)
bar2=Bar()
bar2.add('',stud_info[stud_info['price']==0].course_id.value_counts().index.tolist()[:30]
         ,stud_info[stud_info['price']==0].course_id.value_counts().values.tolist()[:30]
        ,mark_line=['average']
         ,mark_point=['max','min']
         , is_datazoom_show=True)
bar3=Bar()
bar3.add('',stud_info[stud_info['price']!=0].course_id.value_counts().index.tolist()[:15],stud_info[stud_info['price']!=0].course_id.value_counts().values.tolist()[:15]
        ,mark_line=['average']
         ,mark_point=['max','min']
         , is_datazoom_show=True)
stud_info.groupby(['price']).agg({'learn_process':['mean'],'user_id':['count']})
stu_info_course=pd.DataFrame(stud_info.groupby(['course_id']).agg({'learn_process':['mean'],'price':['mean']}))
























