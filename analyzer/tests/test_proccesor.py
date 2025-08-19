
import os
import pytest
import pandas as pd
from analyzer.proccesor import Processor
from analyzer.parser import Parser
from concurrent.futures import ThreadPoolExecutor
import pdb

BASE_DIR = os.path.dirname(__file__)
FILE_PATHS = [
    os.path.join(BASE_DIR, 'data/prints.json'),
    os.path.join(BASE_DIR, 'data/taps.json'),
    os.path.join(BASE_DIR, 'data/pays.csv')
]

def dfs() -> dict[str, pd.DataFrame]:
    with ThreadPoolExecutor() as executor:
            prints_df, taps_df, pays_df = list(executor.map(Parser.parse, FILE_PATHS))
    return {
            "prints": prints_df,
            "taps": taps_df,
            "pays": pays_df
        }

DataFrames = dfs()

class TestProccesor():

    @staticmethod
    def test_get_dates_boundaries():
        dates = Processor.get_dates_boundaries(DataFrames["prints"])
        assert dates['most_recent_day'] == pd.Timestamp('2020-11-30')
        assert dates['one_week_ago'] == pd.Timestamp('2020-11-23')
        assert dates['three_weeks_ago'] == pd.Timestamp('2020-11-09')

    @staticmethod
    def test_get_print_tap_count():
        computed_df = Processor.get_print_tap_count(DataFrames['prints'])
        assert len(computed_df) == 508617
        row_no_prints_in_3_weeks = computed_df[
                                        (computed_df['user_id'] == 98702) &
                                        (computed_df['value_prop'] == 'cellphone_recharge')
                                    ].iloc[0]
        assert row_no_prints_in_3_weeks['user_id'] == 98702
        assert row_no_prints_in_3_weeks['value_prop'] == 'cellphone_recharge'
        assert row_no_prints_in_3_weeks['count'] == 0
        row_with_prints_in_3_weeks = computed_df[
                                        (computed_df['user_id'] == 50807) &
                                        (computed_df['value_prop'] == 'credits_consumer')
                                    ].iloc[0]
        assert row_with_prints_in_3_weeks['user_id'] == 50807
        assert row_with_prints_in_3_weeks['value_prop'] == 'credits_consumer'
        assert row_with_prints_in_3_weeks['count'] == 3
    
    @staticmethod
    def test_get_payments_info():
        computed_payments = Processor.get_payments_info(DataFrames['pays'])
        row_no_payments_in_3_weeks = computed_payments[
                                        (computed_payments['user_id'] == 19321) &
                                        (computed_payments['value_prop'] == 'cellphone_recharge')
                                    ].iloc[0]
        assert row_no_payments_in_3_weeks['user_id'] == 19321
        assert row_no_payments_in_3_weeks['value_prop'] == 'cellphone_recharge'
        assert row_no_payments_in_3_weeks['payments'] == 0
        assert row_no_payments_in_3_weeks['accumulated_amount'] == 0.0
        row_with_1_payment_in_3_weeks = computed_payments[
                                        (computed_payments['user_id'] == 35994) &
                                        (computed_payments['value_prop'] == 'link_cobro')
                                    ].iloc[0]
        assert row_with_1_payment_in_3_weeks['user_id'] == 35994
        assert row_with_1_payment_in_3_weeks['value_prop'] == 'link_cobro'
        assert row_with_1_payment_in_3_weeks['payments'] == 1
        assert row_with_1_payment_in_3_weeks['accumulated_amount'] == 104.70
        row_more_than_1_payment_in_3_weeks = computed_payments[
                                        (computed_payments['user_id'] == 42594) &
                                        (computed_payments['value_prop'] == 'transport')
                                    ].iloc[0]
        assert row_more_than_1_payment_in_3_weeks['user_id'] == 42594
        assert row_more_than_1_payment_in_3_weeks['value_prop'] == 'transport'
        assert row_more_than_1_payment_in_3_weeks['payments'] == 2
        assert row_more_than_1_payment_in_3_weeks['accumulated_amount'] == 213.48
    
    @staticmethod
    @pytest.mark.parametrize("task", ['prints', 'taps', 'payments'])
    def test_dispatch_returns_correct(task, mocker):
        mocked_get_print_tap = mocker.patch('analyzer.proccesor.Processor.get_print_tap_count')
        mocked_get_payments = mocker.patch('analyzer.proccesor.Processor.get_payments_info')
        Processor.dispatch(task, DataFrames['prints'])
        if task == 'prints' or task == 'taps':
            mocked_get_print_tap.assert_called_once()
            mocked_get_payments.assert_not_called()
        elif task == 'payments':
            mocked_get_payments.assert_called_once()
            mocked_get_print_tap.assert_not_called()            

    @staticmethod
    def test_dispatch_raise_exception_on_unknow_task():
        with pytest.raises(Exception) as exc:
            Processor.dispatch('unknown_task', DataFrames['prints'])
        assert "Unknown task: unknown_task" in str(exc.value)

    @staticmethod
    def test_compute_dfs():
        results, dates =  Processor.compute_dfs(FILE_PATHS)
        prints, taps, payments = results

        # Checking prints
        row_no_prints_in_3_weeks = prints[
                                        (prints['user_id'] == 98702) &
                                        (prints['value_prop'] == 'cellphone_recharge')
                                    ].iloc[0]
        assert row_no_prints_in_3_weeks['user_id'] == 98702
        assert row_no_prints_in_3_weeks['value_prop'] == 'cellphone_recharge'
        assert row_no_prints_in_3_weeks['count'] == 0
        row_with_prints_in_3_weeks = prints[
                                        (prints['user_id'] == 50807) &
                                        (prints['value_prop'] == 'credits_consumer')
                                    ].iloc[0]
        assert row_with_prints_in_3_weeks['user_id'] == 50807
        assert row_with_prints_in_3_weeks['value_prop'] == 'credits_consumer'
        assert row_with_prints_in_3_weeks['count'] == 3

        # Checking taps
        row_no_taps_in_3_weeks = taps[
                                        (taps['user_id'] == 98702) &
                                        (taps['value_prop'] == 'cellphone_recharge')
                                    ].iloc[0]
        assert row_no_taps_in_3_weeks['user_id'] == 98702
        assert row_no_taps_in_3_weeks['value_prop'] == 'cellphone_recharge'
        assert row_no_taps_in_3_weeks['count'] == 0
        row_with_taps_in_3_weeks = taps[
                                        (taps['user_id'] == 33643) &
                                        (taps['value_prop'] == 'cellphone_recharge')
                                    ].iloc[0]
        assert row_with_taps_in_3_weeks['user_id'] == 33643
        assert row_with_taps_in_3_weeks['value_prop'] == 'cellphone_recharge'
        assert row_with_taps_in_3_weeks['count'] == 2

        # Checking payments
        row_no_payments_in_3_weeks = payments[
                                        (payments['user_id'] == 19321) &
                                        (payments['value_prop'] == 'cellphone_recharge')
                                    ].iloc[0]
        assert row_no_payments_in_3_weeks['user_id'] == 19321
        assert row_no_payments_in_3_weeks['value_prop'] == 'cellphone_recharge'
        assert row_no_payments_in_3_weeks['payments'] == 0
        assert row_no_payments_in_3_weeks['accumulated_amount'] == 0.0
        row_with_1_payment_in_3_weeks = payments[
                                        (payments['user_id'] == 35994) &
                                        (payments['value_prop'] == 'link_cobro')
                                    ].iloc[0]
        assert row_with_1_payment_in_3_weeks['user_id'] == 35994
        assert row_with_1_payment_in_3_weeks['value_prop'] == 'link_cobro'
        assert row_with_1_payment_in_3_weeks['payments'] == 1
        assert row_with_1_payment_in_3_weeks['accumulated_amount'] == 104.70
        row_more_than_1_payment_in_3_weeks = payments[
                                        (payments['user_id'] == 42594) &
                                        (payments['value_prop'] == 'transport')
                                    ].iloc[0]
        assert row_more_than_1_payment_in_3_weeks['user_id'] == 42594
        assert row_more_than_1_payment_in_3_weeks['value_prop'] == 'transport'
        assert row_more_than_1_payment_in_3_weeks['payments'] == 2
        assert row_more_than_1_payment_in_3_weeks['accumulated_amount'] == 213.48

        # Checking dates
        assert dates['most_recent_day'] == pd.Timestamp('2020-11-30')
        assert dates['one_week_ago'] == pd.Timestamp('2020-11-23')
        assert dates['three_weeks_ago'] == pd.Timestamp('2020-11-09')
    
    @staticmethod
    def test_get_final_df():
        final_df = Processor.get_final_df(FILE_PATHS)
        expected_colums = ['day',
                           'user_id',
                           'value_prop',
                           'prints',
                           'taps',
                           'clicked',
                           'payments',
                           'accumulated_amount']
        
        assert list(final_df.columns) == expected_colums
        assert final_df['day'].between(pd.Timestamp('2020-11-23'), pd.Timestamp('2020-11-30')).all()

        row_with_prints_no_taps_1_payment = final_df[
                                        (final_df['user_id'] == 1) &
                                        (final_df['value_prop'] == 'link_cobro')
                                    ].iloc[0]
        assert row_with_prints_no_taps_1_payment['user_id'] == 1
        assert row_with_prints_no_taps_1_payment['value_prop'] == 'link_cobro'
        assert row_with_prints_no_taps_1_payment['prints'] == 3
        assert row_with_prints_no_taps_1_payment['taps'] == 0
        assert row_with_prints_no_taps_1_payment['clicked'] == 'no'
        assert row_with_prints_no_taps_1_payment['payments'] == 1
        assert row_with_prints_no_taps_1_payment['accumulated_amount'] == 137.14

        row_with_prints_taps_payment = final_df[
                                        (final_df['user_id'] == 99994) &
                                        (final_df['value_prop'] == 'send_money')
                                    ].iloc[0]
        assert row_with_prints_taps_payment['user_id'] == 99994
        assert row_with_prints_taps_payment['value_prop'] == 'send_money'
        assert row_with_prints_taps_payment['prints'] == 2
        assert row_with_prints_taps_payment['taps'] == 1
        assert row_with_prints_taps_payment['clicked'] == 'yes'
        assert row_with_prints_taps_payment['payments'] == 1
        assert row_with_prints_taps_payment['accumulated_amount'] == 139.71

