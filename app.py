"""
异常用户识别 — Streamlit 数据产品
Isolation Forest 粗筛 + 业务规则精筛
"""
import io
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder

st.set_page_config(page_title="异常用户识别", page_icon="", layout="wide")
st.title("异常用户识别")
st.caption("Isolation Forest 无监督检测 + 业务规则精筛 — 识别爬虫、机器人和版权方刷量")

# ==================== 侧边栏：参数配置 ====================
st.sidebar.header("⚙️ 模型参数")
contamination = st.sidebar.slider("异常比例 (contamination)", 0.01, 0.20, 0.05, 0.01,
                                   help="预期数据中异常用户的占比")
n_estimators = st.sidebar.slider("孤立树数量", 50, 500, 200, 50)
max_samples = st.sidebar.select_slider("每棵树最大样本数", [64, 128, 256, 512, 1024], value=512)

st.sidebar.header("📏 精筛规则阈值")
th_launch = st.sidebar.number_input("高频启动阈值 (日均次数)", value=50, min_value=10)
th_valid_ratio = st.sidebar.number_input("卡阈值播放占比", value=0.98, min_value=0.80, max_value=1.0, step=0.01, format="%.2f")
th_play_cnt_valid = st.sidebar.number_input("卡阈值最低播放量", value=30, min_value=5)
th_top1_cp = st.sidebar.number_input("TOP1 CP 占比阈值", value=0.85, min_value=0.50, max_value=1.0, step=0.05, format="%.2f")
th_repeat = st.sidebar.number_input("重复度阈值 (低于此值)", value=0.20, min_value=0.05, max_value=0.50, step=0.05, format="%.2f")
th_play_cnt_repeat = st.sidebar.number_input("刷量最低播放量", value=50, min_value=10)
th_top1_cp_combo = st.sidebar.number_input("CP+卡阈值 CP 占比", value=0.85, min_value=0.50, max_value=1.0, step=0.05, format="%.2f")
th_valid_combo = st.sidebar.number_input("CP+卡阈值播放占比", value=0.98, min_value=0.80, max_value=1.0, step=0.01, format="%.2f")

# ==================== 上传数据 ====================
st.header("📂 数据上传")
uploaded = st.file_uploader("上传特征 CSV（由 features_final.sql 导出）", type=["csv"])

if not uploaded:
    st.info("👆 请上传 CSV 文件开始分析。CSV 需包含 pid 和各行为特征列。")
    st.stop()

# ==================== 数据加载与预处理 ====================
df = pd.read_csv(uploaded)
df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
df = df.dropna(axis=1, how='all')
df = df.drop_duplicates(subset='pid', keep='first')
df = df.drop(columns=['terminal_level'], errors='ignore')

st.success(f"✅ 加载完成：{len(df)} 条用户数据，{df.shape[1]} 个字段")

# ==================== 特征定义 ====================
fea_continuous = [
    'avg_daily_launch_cnt', 'avg_launch_interval', 'std_launch_interval',
    'cv_launch_interval', 'avg_daily_play_cnt', 'avg_daily_valid_play_duration',
    'avg_daily_total_play_duration', 'valid_play_duration_ratio',
    'completion_rate', 'song_repeat_ratio', 'dp_first_launch_ratio',
    'top1_cp_valid_play_ratio', 'cp_diversity_ratio',
]
fea_discrete = ['is_old_version', 'city_level', 'first_launch_mode']

# 检查必要列
missing = [c for c in fea_continuous + fea_discrete if c not in df.columns]
if missing:
    st.error(f"❌ CSV 缺少以下列：{', '.join(missing)}")
    st.stop()

# 离散特征编码
for col in fea_discrete:
    df[col] = LabelEncoder().fit_transform(df[col].astype(str))

features = fea_continuous + fea_discrete
X = df[features].copy()

# ==================== 模型训练 ====================
with st.spinner("🔄 正在运行 Isolation Forest..."):
    n_feat = len(features)
    model = IsolationForest(
        contamination=contamination,
        n_estimators=n_estimators,
        max_samples=max_samples,
        max_features=max(1, int(np.sqrt(n_feat))),
        random_state=42,
    )
    df['anomaly_label'] = model.fit_predict(X)
    df['anomaly_score'] = model.decision_function(X)

df_normal = df[df['anomaly_label'] == 1]
df_outlier = df[df['anomaly_label'] == -1].copy()

# ==================== 规则精筛 ====================
df_outlier['异常类型'] = '未分类'
df_outlier.loc[df_outlier['avg_daily_launch_cnt'] > th_launch, '异常类型'] = '高频启动异常'
df_outlier.loc[df_outlier['dp_first_launch_ratio'] == 1, '异常类型'] = '全是DP拉起'
df_outlier.loc[
    (df_outlier['song_repeat_ratio'] < th_repeat) & (df_outlier['avg_daily_play_cnt'] > th_play_cnt_repeat),
    '异常类型'] = '刷量'
df_outlier.loc[
    (df_outlier['valid_play_duration_ratio'] > th_valid_ratio) & (df_outlier['avg_daily_play_cnt'] > th_play_cnt_valid),
    '异常类型'] = '精准卡有效播放阈值'
df_outlier.loc[
    (df_outlier['top1_cp_valid_play_ratio'] > th_top1_cp) & (df_outlier['song_repeat_ratio'] < 0.25),
    '异常类型'] = 'TOP1 CP集中刷量'
df_outlier.loc[
    (df_outlier['top1_cp_valid_play_ratio'] > th_top1_cp_combo) & (df_outlier['valid_play_duration_ratio'] > th_valid_combo),
    '异常类型'] = 'CP集中+卡阈值'

# ==================== 仪表盘 ====================
st.header("📊 检测结果")

# KPI 卡片
col1, col2, col3, col4 = st.columns(4)
col1.metric("总用户数", f"{len(df):,}")
col2.metric("异常用户（粗筛）", f"{len(df_outlier):,}（{len(df_outlier)/len(df)*100:.1f}%）")
classified = df_outlier[df_outlier['异常类型'] != '未分类']
col3.metric("异常用户（精筛）", f"{len(classified):,}（{len(classified)/len(df)*100:.1f}%）")
col4.metric("未分类", f"{(df_outlier['异常类型']=='未分类').sum():,}")

st.divider()

# 两列布局：异常类型分布 + 异常分数分布
left, right = st.columns(2)

with left:
    st.subheader("异常类型分布")
    type_counts = df_outlier['异常类型'].value_counts()
    type_colors = {
        '高频启动异常': '#E74C3C',
        '全是DP拉起': '#E67E22',
        '刷量': '#F39C12',
        '精准卡有效播放阈值': '#9B59B6',
        'TOP1 CP集中刷量': '#3498DB',
        'CP集中+卡阈值': '#1ABC9C',
        '未分类': '#95A5A6',
    }
    fig_pie = px.pie(values=type_counts.values, names=type_counts.index,
                     color=type_counts.index,
                     color_discrete_map=type_colors,
                     hole=0.4)
    fig_pie.update_traces(textinfo='label+percent', textfont_size=12)
    fig_pie.update_layout(margin=dict(t=20, b=20), showlegend=False)
    st.plotly_chart(fig_pie, use_container_width=True)

with right:
    st.subheader("异常分数分布")
    fig_hist = px.histogram(df, x='anomaly_score', nbins=50,
                            color=df['anomaly_label'].map({1: '正常', -1: '异常'}),
                            color_discrete_map={'正常': '#636EFA', '异常': '#EF553B'},
                            labels={'color': ''})
    fig_hist.update_layout(margin=dict(t=20, b=20), bargap=0.05)
    st.plotly_chart(fig_hist, use_container_width=True)

st.divider()

# 核心特征对比
st.subheader("核心特征对比（正常 vs 异常）")
key_features = ['cv_launch_interval', 'top1_cp_valid_play_ratio', 'cp_diversity_ratio',
                'valid_play_duration_ratio', 'song_repeat_ratio', 'avg_daily_launch_cnt']

cols = st.columns(3)
for i, feat in enumerate(key_features):
    with cols[i % 3]:
        fig_box = go.Figure()
        fig_box.add_trace(go.Box(y=df_normal[feat], name='正常', marker_color='#636EFA'))
        fig_box.add_trace(go.Box(y=df_outlier[feat], name='异常', marker_color='#EF553B'))
        fig_box.update_layout(title=feat, height=300, margin=dict(t=40, b=20),
                              showlegend=False)
        st.plotly_chart(fig_box, use_container_width=True)

st.divider()

# 各规则命中统计
st.subheader("精筛规则命中统计")
rules_summary = {
    f'精准卡阈值刷量 (valid>{th_valid_ratio} & play>{th_play_cnt_valid})':
        ((df_outlier['valid_play_duration_ratio'] > th_valid_ratio) & (df_outlier['avg_daily_play_cnt'] > th_play_cnt_valid)).sum(),
    f'高频启动异常 (launch>{th_launch})':
        (df_outlier['avg_daily_launch_cnt'] > th_launch).sum(),
    f'TOP1 CP集中刷量 (top1>{th_top1_cp} & repeat<0.25)':
        ((df_outlier['top1_cp_valid_play_ratio'] > th_top1_cp) & (df_outlier['song_repeat_ratio'] < 0.25)).sum(),
    '全是DP拉起 (dp=1)':
        (df_outlier['dp_first_launch_ratio'] == 1).sum(),
    f'刷量 (repeat<{th_repeat} & play>{th_play_cnt_repeat})':
        ((df_outlier['song_repeat_ratio'] < th_repeat) & (df_outlier['avg_daily_play_cnt'] > th_play_cnt_repeat)).sum(),
    f'CP集中+卡阈值 (top1>{th_top1_cp_combo} & valid>{th_valid_combo})':
        ((df_outlier['top1_cp_valid_play_ratio'] > th_top1_cp_combo) & (df_outlier['valid_play_duration_ratio'] > th_valid_combo)).sum(),
}
rules_df = pd.DataFrame({'规则': rules_summary.keys(), '命中数': rules_summary.values()})
fig_bar = px.bar(rules_df, x='命中数', y='规则', orientation='h',
                 color_discrete_sequence=['#FF6B6B'])
fig_bar.update_layout(height=300, margin=dict(t=10, b=10, l=10), yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# 异常用户明细
st.subheader("异常用户明细")
show_cols = ['pid', 'anomaly_score', '异常类型', 'avg_daily_play_cnt',
             'top1_cp_valid_play_ratio', 'valid_play_duration_ratio',
             'song_repeat_ratio', 'dp_first_launch_ratio', 'avg_daily_launch_cnt']

type_filter = st.multiselect("按异常类型筛选", df_outlier['异常类型'].unique().tolist(),
                              default=df_outlier['异常类型'].unique().tolist())
filtered = df_outlier[df_outlier['异常类型'].isin(type_filter)].sort_values('anomaly_score')
st.dataframe(filtered[show_cols], use_container_width=True, height=400)

# 导出
st.download_button(
    "📥 导出异常用户 CSV",
    filtered[show_cols].to_csv(index=False).encode('utf-8-sig'),
    file_name="anomaly_users.csv",
    mime="text/csv",
)
