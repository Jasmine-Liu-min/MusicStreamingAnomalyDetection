-- 异常检测（爬虫/机器人/版权方刷量）特征提取

-- 统计周期：近 7 天，参数 ${a_date} 为统计截止日（yyyymmdd）

-- 数据源：
--   启动表:     dw.app_launch_log
--   播放表:     dw.play_song_log
--   歌曲维表:   dim.song_info
--   设备维表:   dim.device_info
--   用户画像表: dw.user_profile
--   版本维表:   dim.app_version
-- ===========================================================

-- 异常检测（爬虫/机器人/版权方刷量）特征提取

-- 统计周期：近 7 天，参数 ${a_date} 为统计截止日（yyyymmdd）

-- 数据源：
--   启动表:     dw.app_launch_log
--   播放表:     dw.play_song_log
--   歌曲维表:   dim.song_info
--   设备维表:   dim.device_info
--   用户画像表: dw.user_profile
--   版本维表:   dim.app_version

-- 启动表 dw.app_launch_log
-- avg_daily_launch_cnt（单日平均启动次数）
-- cv_launch_interval（启动间隔变异系数）
-- first_launch_source（首次启动来源）
-- app_version 

-- 1.启动表计算单日平均启动次数：
-- 总启动次数/有记录的天数，count(1)/count(distinct date)
-- 启动间隔变异系数:先算每两次相邻启动的时间间隔，再算这些间隔的标准差、均值、变异系数
-- 首次启动来源（通过app_launch_mode）：取这个pid这一天最早一条启动记录的app_launch_mode

with 
-- 1.启动行为特征，从app启动事实表
launch_base as(
    SELECT
    pid,
    CAST(TO_DATE(p_date, 'yyyymmdd') AS DATE) as a_date,
    app_launch_time,
    app_version,
    app_launch_mode,
    datediff(
        app_launch_time,
        lag(app_launch_time) over(partition by pid,cast(TO_DATE(p_date,'yyyymmdd') as date) order by app_launch_time),'ss'
    ) as launch_interval_sec
    from dw.app_launch_log
    WHERE CAST(TO_DATE(p_date, 'yyyymmdd') AS DATE) >= DATE_ADD(TO_DATE('${a_date}', 'yyyymmdd'), -7)
      AND CAST(TO_DATE(p_date, 'yyyymmdd') AS DATE) <  CAST(TO_DATE('${a_date}', 'yyyymmdd') AS DATE)
      AND pid IS NOT NULL AND pid != ''
    ),

-- 每天每个pid的启动次数、间隔统计指标
launch_daily as(
    select pid,
    a_date,
    count(1) AS daily_launch_cnt,
    avg(launch_interval_sec) as daily_avg_interval,
    -- 标准差
    stddev(launch_interval_sec) as daily_std_interval,
    case WHEN avg(launch_interval_sec)>0 
    then stddev(launch_interval_sec)/avg(launch_interval_sec) else 0 
    end as daily_cv_interval 
    from launch_base 
    group by pid,a_date
),

-- 每天每个pid的首次启动来源
first_launch_daily as(
    select
    pid,
    a_date,
    app_launch_mode as first_launch_mode,
    row_number() over(partition by pid,a_date order by app_launch_time asc) as rn
    from launch_base
),
first_launch as(
    select pid,a_date,first_launch_mode
    from first_launch_daily 
    where rn=1
),

-- 第二层启动行为，聚合到pid粒度
launch_features as(
    select 
    pid,
    sum(daily_launch_cnt)/count(distinct a_date) as avg_daily_launch_cnt,
    AVG(daily_avg_interval) AS avg_launch_interval,
    AVG(daily_std_interval) AS std_launch_interval,
    AVG(daily_cv_interval) AS cv_launch_interval
    from launch_daily 
    group by pid 
),

--首次启动来源众数
first_launch_cnt as(
    select pid,
    first_launch_mode,
    count(1) as cnt,
    row_number() over(partition by pid order by count(1) desc) as rn 
    from first_launch
    group by pid,first_launch_mode
),
first_launch_mode_features as(
    select pid,first_launch_mode 
    from first_launch_cnt
    where rn=1
),

--dp拉起作为首次启动来源的天数占比(app_launch_mode=4) 
dp_launch_features as(
    select
    pid,
    sum(case when first_launch_mode=4 then 1 else 0 end)/count(1) as dp_first_launch_ratio
    from first_launch 
    group by pid
),

-- 取最近一次的app_version
launch_latest as(
    select pid,
    app_version,
    a_date,
    row_number() over(partition by pid order by a_date desc,app_launch_time desc) as rn 
    from launch_base
),
latest_info as(
    select pid,app_version, a_date AS last_active_date
    from launch_latest 
    where rn=1
),

-- 播放表 dw.play_song_log
-- avg_daily_play_cnt（单日播放歌曲总数）
-- avg_daily_valid_play_duration（单日有效播放时长）
-- avg_daily_total_play_duration（单日播放总时长）
-- valid_play_duration_ratio（有效播放时长/总播放时长）
-- completion_rate（完播率）
-- song_repeat_ratio（歌曲重复度）

-- 2.播放特征：
-- 先pid+day，统计当天播放歌曲数、当天有效播放时长、当天播放总时长、当天有效播歌时长/总播放时长、当天完播率、当天歌曲重复度
-- 再avg

play_daily as(
    select
    pid,
    CAST(TO_DATE(p_date, 'yyyymmdd') AS DATE) AS a_date,
    count(1) as daily_play_cnt,
    sum(case when is_valid_play_song=1 then play_duration else 0 end) as daily_valid_play_duration,
    SUM(play_duration) AS daily_total_play_duration,
    case when sum(play_duration)>0
    then sum(case when is_valid_play_song=1 then play_duration else 0 end)/sum(play_duration)
    end as daily_valid_ratio,
    SUM(CASE WHEN is_full_play_song = 1 THEN 1 ELSE 0 END)
            / COUNT(1) AS daily_completion_rate,
    count(distinct song_id)/count(1) as daily_song_repeat_ratio--当天去重歌曲数 / 当天总播放次数,值越低，说明重复播放越多
    FROM dw.play_song_log
    WHERE CAST(TO_DATE(p_date, 'yyyymmdd') AS DATE) >= DATE_ADD(TO_DATE('${a_date}', 'yyyymmdd'), -7)
      AND CAST(TO_DATE(p_date, 'yyyymmdd') AS DATE) < CAST(TO_DATE('${a_date}', 'yyyymmdd') AS DATE)
      AND pid IS NOT NULL AND pid != ''
    GROUP BY pid, CAST(TO_DATE(p_date, 'yyyymmdd') AS DATE)
),

--聚合到pid粒度
play_features as(
    select pid,
    sum(daily_play_cnt)/count(distinct a_date) as avg_daily_play_cnt,
    sum(daily_valid_play_duration)/count(distinct a_date) as avg_daily_valid_play_duration,
    sum(daily_total_play_duration)/count(distinct a_date) as avg_daily_total_play_duration,
    avg(daily_valid_ratio) as valid_play_duration_ratio,
    avg(daily_completion_rate) as completion_rate,
    avg(daily_song_repeat_ratio) as song_repeat_ratio
    from play_daily 
    group by pid
),

-- 播放表 + 歌曲维表 dim.song_info
-- top1_cp_valid_play_ratio（Top1 CP有效播放次数占比）
-- cp_diversity_ratio（CP分散度）
--3.CP相关

--先按pid+天+cp分组，计算出每个CP当天的有效播放次数
play_with_cp as(
    select 
    p.pid,
    CAST(TO_DATE(p.p_date, 'yyyymmdd') AS DATE) AS a_date,
    p.is_valid_play_song,
    s.cppartner_id
    from dw.play_song_log p
    left join dim.song_info s 
    on p.song_id=s.song_id 
    and s.end_date=99991231
    WHERE CAST(TO_DATE(p.p_date, 'yyyymmdd') AS DATE) >= DATE_ADD(TO_DATE('${a_date}', 'yyyymmdd'), -7)
      AND CAST(TO_DATE(p.p_date, 'yyyymmdd') AS DATE) < CAST(TO_DATE('${a_date}', 'yyyymmdd') AS DATE)
      AND p.pid IS NOT NULL AND p.pid != ''
),
cp_daily_a as(
    select pid,
    a_date,
    cppartner_id,
    count(1) as cp_play_cnt,
    sum(case when is_valid_play_song=1 then 1 else 0 end) as cp_valid_play_cnt
    from play_with_cp 
    where cppartner_id is not null 
    group by pid,a_date,cppartner_id
),

cp_daily AS (
    SELECT
        pid, a_date,
        CASE WHEN SUM(cp_valid_play_cnt) > 0
            THEN CAST(MAX(cp_valid_play_cnt) AS DOUBLE) / SUM(cp_valid_play_cnt)
            ELSE 0
        END AS daily_top1_cp_ratio,
       COUNT(DISTINCT cppartner_id) / CASE 
    -- 分母（总播放量）等于0时，返回NULL，避免【除以0报错】
    WHEN SUM(cp_play_cnt) = 0 THEN NULL 
    -- 分母不为0时，正常用总播放量做除数
    ELSE SUM(cp_play_cnt) 
END AS daily_cp_diversity  --值越低说明 CP 越集中
    FROM cp_daily_a
    GROUP BY pid, a_date
),

-- 聚合成 pid 粒度
cp_features AS (
    SELECT
        pid,
        AVG(daily_top1_cp_ratio) AS top1_cp_valid_play_ratio,
        AVG(daily_cp_diversity) AS cp_diversity_ratio
    FROM cp_daily
    GROUP BY pid
),

-- 4.设备维表 dim.device_info
-- terminal_level（设备档次）
-- ip_city
pid_features AS (
    SELECT pid, terminal_level
    FROM dim.device_info
    WHERE pid IS NOT NULL AND pid != ''
    and p_date=${a_date}
),

-- 5.版本维表 dim.app_version
-- is_old_version（是否老版本），
version_features AS (
    SELECT
        li.pid,
        CASE WHEN v.app_version_release_date IS NOT NULL
                AND DATEDIFF(
                    li.last_active_date ,
                   CAST(TO_DATE(v.app_version_release_date,'yyyy-mm-dd') AS DATE),
                    'dd'
                    ) > 30
            THEN 1
            ELSE 0
        END AS is_old_version
    FROM latest_info li
    LEFT JOIN dim.app_version v
        ON li.app_version = v.app_version
        AND v.is_valid = 1
        AND v.app_version_type = 4
),

-- 用户画像表 dw.user_profile
-- age_group（年龄段）
user_profile AS (
    SELECT pid, age_group, city_level
    FROM dw.user_profile
    WHERE pid IS NOT NULL AND pid != ''
    and p_date=${a_date}
)

-- 汇总，pid维度
SELECT
    l.pid,

    -- ===== 基础行为特征 =====
    l.avg_daily_launch_cnt,
    l.avg_launch_interval,
    l.std_launch_interval,
    l.cv_launch_interval,
    p.avg_daily_play_cnt,
    p.avg_daily_valid_play_duration,
    p.avg_daily_total_play_duration,
    p.valid_play_duration_ratio,
    p.completion_rate,
    p.song_repeat_ratio,

    -- ===== 设备/环境特征 =====
    dv.terminal_level,
    vr.is_old_version,
    u.city_level,
    u.age_group,
    fl.first_launch_mode,
    dp.dp_first_launch_ratio,

    -- ===== 版权方刷量特征 =====
    c.top1_cp_valid_play_ratio,
    c.cp_diversity_ratio

FROM launch_features l
LEFT JOIN play_features p ON l.pid = p.pid
LEFT JOIN cp_features c ON l.pid = c.pid
LEFT JOIN latest_info li ON l.pid = li.pid
LEFT JOIN pid_features dv ON l.pid = dv.pid
LEFT JOIN version_features vr ON l.pid = vr.pid
LEFT JOIN first_launch_mode_features fl ON l.pid = fl.pid
LEFT JOIN dp_launch_features dp ON l.pid = dp.pid
LEFT JOIN user_profile u ON l.pid = u.pid

WHERE l.avg_launch_interval IS NOT NULL
  AND p.avg_daily_play_cnt IS NOT NULL
  AND c.top1_cp_valid_play_ratio IS NOT NULL
  AND dv.terminal_level IS NOT NULL
  AND vr.is_old_version IS NOT NULL
  AND u.city_level IS NOT NULL
  AND u.age_group IS NOT NULL
  AND fl.first_launch_mode IS NOT NULL
  AND dp.dp_first_launch_ratio IS NOT NULL