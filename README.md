# 异常用户识别

基于 **Isolation Forest 无监督检测 + 业务规则精筛** 的异常用户识别系统，用于识别爬虫、机器人和版权方刷量等异常行为用户。

## 项目背景

在用户增长场景中，异常用户（如爬虫、自动化脚本、版权方刷量账号）会干扰数据指标的真实性。本项目通过构建用户行为特征，利用 Isolation Forest 进行无监督异常检测粗筛，再结合业务规则进行精筛分类，最终输出异常用户清单。

## 技术方案

### 整体流程

```
SQL 特征提取 → CSV 数据导出 → Isolation Forest 粗筛 → 业务规则精筛 → 可视化分析 & 导出
```

### 1. 特征工程（`features_final.sql`）

从数仓多张表中提取用户行为特征，统计周期为近 7 天：

| 特征类别 | 特征 | 说明 |
|---------|------|------|
| 启动行为 | `avg_daily_launch_cnt` | 日均启动次数 |
| | `avg_launch_interval` / `std_launch_interval` / `cv_launch_interval` | 启动间隔均值、标准差、变异系数 |
| | `first_launch_mode` | 首次启动来源众数 |
| | `dp_first_launch_ratio` | DP 拉起作为首次启动的天数占比 |
| 播放行为 | `avg_daily_play_cnt` | 日均播放次数 |
| | `valid_play_duration_ratio` | 有效播放时长占比 |
| | `completion_rate` | 完播率 |
| | `song_repeat_ratio` | 歌曲重复度（越低越异常） |
| CP 相关 | `top1_cp_valid_play_ratio` | Top1 CP 有效播放占比 |
| | `cp_diversity_ratio` | CP 分散度 |
| 设备/环境 | `is_old_version` / `city_level` / `terminal_level` | 版本、城市等级、设备档次 |

数据源涉及：启动表、播放表、歌曲维表、设备维表、用户画像表、版本维表。

### 2. 异常检测模型

**Isolation Forest** 无监督检测，核心参数可通过 Streamlit 侧边栏调节：

- `contamination`：预期异常比例（默认 5%）
- `n_estimators`：孤立树数量（默认 200）
- `max_samples`：每棵树最大样本数（默认 512）

### 3. 精筛规则

对粗筛出的异常用户进行业务分类：

| 异常类型 | 规则逻辑 |
|---------|---------|
| 高频启动异常 | 日均启动次数 > 50 |
| 全是 DP 拉起 | DP 首次启动占比 = 100% |
| 刷量 | 歌曲重复度 < 0.20 且日均播放 > 50 |
| 精准卡有效播放阈值 | 有效播放占比 > 0.98 且日均播放 > 30 |
| TOP1 CP 集中刷量 | Top1 CP 占比 > 0.85 且重复度 < 0.25 |
| CP 集中 + 卡阈值 | Top1 CP 占比 > 0.85 且有效播放占比 > 0.98 |

所有阈值均可在 Streamlit 侧边栏实时调整。

## 项目结构

```
├── app.py                              # Streamlit 可视化应用（主入口）
├── sql/
│   └── features_final.sql              # 特征提取 SQL
├── notebook/
│   └── anomaly_detection_IF.ipynb      # 探索性分析 Notebook
├── data/                               # 放置 SQL 导出的特征 CSV（已 gitignore）
├── requirements.txt
└── README.md
```

## 快速开始

### Windows (PowerShell)

```powershell
pip install -r requirements.txt
streamlit run app.py
```

### WSL / Linux

```bash
pip install -r requirements.txt
python -m streamlit run app.py --server.headless true --server.port 8501
```

依赖：pandas, numpy, scikit-learn, streamlit, plotly

启动后在浏览器中打开，上传由 `features_final.sql` 导出的特征 CSV 文件即可开始分析。

### 使用流程

1. 在数仓中执行 `features_final.sql`，导出用户特征 CSV
2. 启动 Streamlit 应用，上传 CSV
3. 在侧边栏调整模型参数和精筛阈值
4. 查看检测结果仪表盘
5. 导出异常用户清单

## Streamlit 仪表盘（`app.py`）

上传 CSV 后自动运行模型，页面展示：

- **KPI 卡片**：总用户数、粗筛异常数、精筛异常数、未分类数
- **异常类型饼图**：各类异常占比分布
- **异常分数直方图**：正常 vs 异常用户的分数分布对比
- **核心特征箱线图**：cv_launch_interval、top1_cp_valid_play_ratio、cp_diversity_ratio、valid_play_duration_ratio、song_repeat_ratio、avg_daily_launch_cnt 六个关键特征的正常/异常对比
- **规则命中柱状图**：各精筛规则分别命中多少用户
- **异常用户明细表**：支持按异常类型筛选，按异常分数排序，可导出 CSV

左侧边栏可实时调节模型参数（contamination、树数量、样本数）和所有精筛规则阈值。
