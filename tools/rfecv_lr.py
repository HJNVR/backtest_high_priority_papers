import numpy as np
import pandas as pd
import json
import os
from itertools import product
from sklearn.feature_selection import RFECV
from dateutil.relativedelta import relativedelta
from model_selection import rolling_valid_split
from sklearn.linear_model import LogisticRegression
import warnings; warnings.filterwarnings('ignore')

args = json.load(open('../config/back_test.json', 'r'))
if not os.path.exists('../result/'):
    os.makedirs('./result/')

y_col_prefix = args['y_col_prefix']
pred_col = 'pred_ret'
dummy_cols = args['primary_key']
kfold = args['kfold']
train_end_ym = args['first_train_end_ym']
tune_sampler = args['tune_sampler']
md = args['md']

# this data can be automatically generated by yh_data.py in tools
daily_prc = pd.read_csv('../data/sp500_daily_prc.csv')
daily_prc['date'] = pd.to_datetime(daily_prc['date'])
daily_prc['year'] = daily_prc['date'].dt.year
daily_prc['month'] = daily_prc['date'].dt.month
daily_prc = daily_prc.sort_values(['ticker', 'date']).reset_index(drop=True)
# legal ticker at certain month should be sold for a specific period (fixed length)
counting_max =  daily_prc.groupby(['ticker', 'year', 'month'])['date'].count().reset_index()
# counting_max will be used to validate ticker when selecting top-k tickers
counting_max = counting_max.groupby(['year', 'month'])['date'].max()

df = pd.read_csv('../data/consolidated_features.csv', index_col = [0])
# processing the raw data
df['date'] = pd.to_datetime(df['date'])
# rename the `date` in origin data to `trade_date`
df['trade_date'] = df['date'].values
y_cols = [f'fut_ret{col}' for col in args['test_period']]
dummy_cols = dummy_cols + ['trade_date']
feat_cols = [col for col in df.columns if col not in (dummy_cols + y_cols)]
# new `date` will be used to trace the back test process
df['date'] = df['date'] + pd.tseries.offsets.MonthEnd(n=0) - pd.tseries.offsets.MonthBegin(n=1)

# TODO fillna [Feature Engineer should do]
df.replace([np.inf, -np.inf], np.nan, inplace=True)
df[feat_cols] = df[feat_cols].fillna(0)

train_end_date = pd.to_datetime(train_end_ym, format='%Y%m')
money = args['money']
pf_daily_trend = pd.DataFrame()
# record all the important results in a csv file
f = open(f'../result/final_summary.csv', 'w', encoding='utf-8')
f.write('start date,end date,train_valid_period(years),test_period(months),train_period(years),topk,feature_selection,Annual Return,MDD,Calmar Ratio\n')

while train_end_date < df['date'].max():
    print(f'[combo selection]End date of training is: {train_end_date}')
    combo_cnt = 0
    # choose combination of train_valid_date, test_period:
    calmar_ratio_res = pd.DataFrame({
        'train_valid_period':[], 'test_period': [],
        'train_period': [], 'topk': [], 'fs': [], 'cr': []
    })
    ar_res = pd.DataFrame({
        'train_valid_period':[],'test_period': [],
        'train_period': [], 'topk': [], 'fs': [], 'ar': []
    })
    mdd_res = pd.DataFrame({
        'train_valid_period':[], 'test_period': [],
        'train_period': [], 'topk': [], 'fs': [], 'mdd': []
    })

    pf_daily_res = {}
    fund_res = {}

    init_fund = money

    for combo in product(args['train_valid_period'], args['test_period'], args['train_period'], args['topk'], args['fs']):
        print('current combination:', combo)
        train_valid_period = combo[0]
        test_period = combo[1]
        train_period = combo[2]
        topk = combo[3]
        fs_tool = combo[4]
        valid_period = test_period
        # change year to month
        train_valid_period *= 12
        train_period *= 12

        y_col = f'{y_col_prefix}{test_period}'
        data = df[dummy_cols + feat_cols + [y_col]].copy()
        data = data.dropna()
        actual_train_end_date = train_end_date - relativedelta(months=test_period - 1)

        train_start_date = train_end_date - relativedelta(months=train_valid_period)
        predict_end_date = train_end_date + relativedelta(months=test_period)
        predict_dates = pd.date_range(
            start=train_end_date + relativedelta(months=1), end=min(predict_end_date, df['date'].max()), freq='MS')

        valid_date_sets = rolling_valid_split(kfold, train_period, train_start_date, actual_train_end_date, valid_period)
        train_data = data.query(f'"{train_start_date}" < date <= "{actual_train_end_date}"')
        X_train = train_data[feat_cols].values
        y_train = train_data[y_col].values
        print(X_train)
        print(y_train)
        break
  	break
        '''
        # implement feature selection, 'mi', 'boruta', 'ae', ''
        _, select_ids = feature_selection(X_train, y_train, method=fs_tool, k=25)
        if sum(select_ids) == 0:
            select_ids = [True for _ in feat_cols] # TODO

        def objective(trial):
            param = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 500),   
                'num_leaves': trial.suggest_int('num_leaves', 10, 512),
                'min_data_in_leaf': trial.suggest_int('min_data_in_leaf', 10, 80),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.0, 1.0), # subsample
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1),  # eta
                'lambda_l1': trial.suggest_float('lambda_l1', 0.01, 1),  # reg_alpha
                'lambda_l2': trial.suggest_float('lambda_l2', 0.01, 1), # reg_lambda
            }
            kpis = []
            for valid_train_start_date, valid_train_end_date, valid_end_date in valid_date_sets: 
                fold_train_data = data.query(f'"{valid_train_start_date}" < date <= "{valid_train_end_date}"')
                fold_valid_data = data.query(f'"{valid_train_end_date}" < date <= "{valid_end_date}"')
                train_x = fold_train_data[feat_cols].values[:, select_ids]
                train_y = fold_train_data[y_col].values
                val_x = fold_valid_data[feat_cols].values[:, select_ids]
                val_y = fold_valid_data[y_col].values
                model = LGBMRegressor(seed=42, **param)
                model.fit(train_x, train_y)
                y_pred = model.predict(val_x)
                kpi = oos_rsquare(val_y, y_pred)
                kpis.append(kpi)
            return np.mean(kpis)
        
        study = optuna.create_study(
            direction="maximize", 
            sampler=sampler, 
            study_name=f'train_end_date_{train_end_date}'
        )
        study.optimize(objective, n_trials=args['tune_trials'])
        # use best hyperparameters to train this part of back test
        param = study.best_params
        model = LGBMRegressor(seed=42, **param)
        model.fit(X_train[:, select_ids], y_train)

        # record investment changes for next `test_period` months
        # run prediction in the first predict month and rebalance the investment after `test_period` month
        # print('--------\n', predict_dates, '\n--------\n')
        predict_date = predict_dates[0]
        test_data = data.query(f'date == "{predict_date}"')
        X_test = test_data[feat_cols].values[:, select_ids]
        y_test = test_data[y_col].values

        outputs = test_data[dummy_cols + [y_col]].copy()
        outputs[pred_col] = model.predict(X_test)

        # descending sort values to select topk-stocks
        outputs = outputs.sort_values(by=[pred_col], ascending=False)
        # find out the legal ticker will have how many days recording during `test_period` months
        ideal_ser_num = sum([counting_max[(pdt.year, pdt.month)] for pdt in predict_dates])
        # equally separate investment to `topk` folds
        allocate_fund = init_fund / topk
        # store the portfolio value changing records during next `test_period` months
        ticker_recs_this_month = []
        # store the expected portfolio value for next rebalance strategy
        new_money = 0.
        idx, cnt = 0, 0
        tickers_tmp_info = []
        while cnt < topk:
            # select the ticker name with rank number `idx`
            ticker_name = outputs.iloc[idx, :]['ticker']
            # select the daily changing prices of this ticker
            et = predict_dates[-1] + pd.tseries.offsets.MonthEnd(n=1)
            ticker_ts = daily_prc.query(
                f'"{str(predict_dates[0].date())}" <= date <= "{str(et.date())}" and ticker == "{ticker_name}"')
            if len(ticker_ts) != ideal_ser_num:
                # this ticker is illegal, just skip and find next.
                idx += 1
                continue
            else:
                # legal ticker, just sort by datetime again for insurance
                ticker_ts = ticker_ts.sort_values(by=['date']).reset_index(drop=True) 
                # the first price in ticker_ts will be the purchase price of this ticker
                purchase_price = ticker_ts.head(1)['adjclose'].values[0]
                purchase_amount = allocate_fund / purchase_price
                # record daily price for this ticker, this list will store value changes for each top-k ticker
                # it will be summed in next code to represent the portfolio changes
                ticker_recs_this_month.append(purchase_amount * ticker_ts['adjclose'].values)
                # the last price in ticker_ts will be the selling price of this ticker
                sell_price = ticker_ts.tail(1)['adjclose'].values[0]
                sell_return = sell_price * purchase_amount
                # add the return money of this ticker after `test_period` months
                new_money += sell_return
                tickers_tmp_info.append(ticker_name)
                cnt += 1
                idx += 1
        selected_pf_daily = pd.DataFrame({
            'date': ticker_ts['date'].values, 
            'val': np.sum(ticker_recs_this_month, axis=0)}) 
        t1, t2 = selected_pf_daily['date'].min(), selected_pf_daily['date'].max()
        y_num = (t2 - t1).days / 365
        ar = round(np.power(selected_pf_daily['val'].iloc[-1] / selected_pf_daily['val'].iloc[0], 1 / y_num) - 1, 4)
        selected_pf_daily['cummax'] = selected_pf_daily['val'].cummax()
        selected_pf_daily['drawdown'] = (selected_pf_daily['val'] - selected_pf_daily['cummax']) / selected_pf_daily['cummax']
        mdd = round(selected_pf_daily['drawdown'].min(), 4)
        cr = ar / abs(mdd)

        calmar_ratio_res.loc[combo_cnt, :] = [train_valid_period//12, test_period, train_period//12, topk, fs_tool, cr]
        ar_res.loc[combo_cnt, :] = [train_valid_period//12, test_period, train_period//12, topk, fs_tool, ar]
        mdd_res.loc[combo_cnt, :] = [train_valid_period//12, test_period, train_period//12, topk, fs_tool, mdd]
        

        line = f'{str(t1.date())},{str(t2.date())},{train_valid_period//12},{test_period},{train_period//12},{topk},{fs_tool},{ar},{mdd},{cr}\n'
        f.write(line)
        f.flush()

        pf_daily_res[combo_cnt] = selected_pf_daily[['date', 'val']].copy()
        fund_res[combo_cnt] = new_money
        combo_cnt += 1
    
    best_combo = calmar_ratio_res.query(f'cr == {calmar_ratio_res.cr.max()}').index[0]
    best_test_perid = calmar_ratio_res.loc[best_combo, 'test_period']
    attach_pf_daily = pf_daily_res[best_combo]
    money = fund_res[best_combo]
    pf_daily_trend = pd.concat([pf_daily_trend, attach_pf_daily], ignore_index=True)
    # # draw combo-CR hearmap
    # fig, ax = plt.subplots(1, 3, figsize=(20, 5))
    # sns.heatmap(
    #     ar_res.pivot("train_valid_period", "test_period", "ar"), 
    #     ax=ax[0], cbar_kws={'format': '%.0f%%'}, 
    #     cmap=sns.color_palette("Blues", as_cmap=True))
    # ax[0].set_title(f'Portfolio Annual Return', fontsize=16, fontweight='bold', loc='left')
    # sns.heatmap(
    #     mdd_res.pivot("train_valid_period", "test_period", "mdd"), 
    #     ax=ax[1], cbar_kws={'format': '%.0f%%'}, 
    #     cmap=sns.color_palette("ch:start=.2,rot=-.3", as_cmap=True))
    # ax[1].set_title(f'Portfolio Max DrawDown', fontsize=16, fontweight='bold', loc='left')
    # sns.heatmap(
    #     calmar_ratio_res.pivot("train_valid_period", "test_period", "cr"), ax=ax[2])
    # ax[2].set_title(f'Portfolio Calmar Ratio', fontsize=16, fontweight='bold', loc='left')
    # for i in range(3):
    #     ax[i].set_ylabel('train_valid_period', fontweight='bold', fontsize=14)
    #     ax[i].set_xlabel('test_period', fontweight='bold', fontsize=14)

    # sdt, edt = attach_pf_daily['date'].min(), attach_pf_daily['date'].max()
    # ax[0].text(
    #     x=0., y=1.13, 
    #     s=f'{str(sdt.date())} ~ {str(edt.date())}', 
    #     fontsize=18, fontweight='bold', ha='left', va='top', transform=ax[0].transAxes)

    # plt.tight_layout()
    # plt.savefig(f'./result/heatmap_summary_from_{sdt.year}{sdt.month:02d}_to_{edt.year}{edt.month:02d}.png', format='png')
    # plt.close()

    # to next back test part
    train_end_date = train_end_date + relativedelta(months=best_test_perid)
# finish recording, close the csv file
f.close()
'''
