from datetime import datetime, timedelta
import json
import os
import re
import telebot
from telebot import types
import pandas as pd
import matplotlib.pyplot as plt
from telebot.types import CallbackQuery
import plotly.graph_objects as go
import config

bot = telebot.TeleBot(config.token)
main_path = 'data'
main_path = config.data_path
list_devices = os.listdir(main_path)


def load_json(path):
    return json.load(open(path, 'r'))


def upload_json(path, to_save):
    with open(path, 'w') as outfile:
        json.dump(to_save, outfile)


@bot.message_handler(commands=['preprocessing_all_files'])
def preprocessing_all_files(message):
    for path in list_devices:
        for file in os.listdir(f'{main_path}/{path}'):
            if file.endswith('.csv'):
                preprocessing_one_file(f'{main_path}/{path}/{file}')


def preprocessing_one_file(path):
    _, device, file_name = path.split('/')
    
    if device not in ["AE33-S09-01249", "LVS", "PNS", "TCA08", "Web_MEM"]:
        return
        
    print(path)
    df = pd.read_csv(path, sep=None, engine='python')
    time_col = load_json('config_devices.json')[device]['time_cols']
    if device == "AE33-S09-01249":
        df[time_col] = pd.to_datetime(df[time_col], format="%d.%m.%Y %H:%M")
    elif device == "LVS" or device == "PNS":
        col = list(df.columns)
        df = df.drop('Error', axis=1)
        col.remove("Time")
        df.columns = col
        df[time_col] = pd.to_datetime(df[time_col], format="%d.%m.%Y %H:%M:%S")
    elif device == "TCA08":
        df[time_col] = pd.to_datetime(df[time_col], format="%Y-%m-%d %H:%M:%S")
    elif device == "Web_MEM":
        df[time_col] = pd.to_datetime(df[time_col], format="%d.%m.%Y %H:%M")
    else:
        return
        
    cols_to_draw = load_json('config_devices.json')[device]['cols']
    time_col = load_json('config_devices.json')[device]['time_cols']
    df = df[cols_to_draw + [time_col]]
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    name = re.split("[-_]", file_name)
    if not os.path.exists(f'proc_data/{device}'):
        os.makedirs(f'proc_data/{device}')
    df = df.sort_values(by=time_col)
    diff_mode = df[time_col].diff().mode().values[0] * 1.1
    new_rows = []
    for i in range(len(df) - 1):
        diff = (df.loc[i + 1, time_col] - df.loc[i, time_col])
        if diff > diff_mode:
            new_date1 = df.loc[i, time_col] + pd.Timedelta(seconds=1)
            new_date2 = df.loc[i + 1, time_col] - pd.Timedelta(seconds=1)
            new_row1 = {time_col: new_date1}
            new_row2 = {time_col: new_date2}
            new_rows.extend([new_row1, new_row2])
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    df = df.sort_values(by=time_col)
    df.to_csv(f'proc_data/{device}/{name[0]}_{name[1]}.csv', index=False)
    return f'proc_data/{device}/{name[0]}_{name[1]}.csv'


@bot.message_handler(commands=['start'])
def start(message):
    d = load_json('user_info.json')
    if str(message.from_user.id) not in d.keys():
        d[str(message.from_user.id)] = {}
    d[str(message.from_user.id)]['update_quick_access'] = False
    d[str(message.from_user.id)].pop('selected_columns', None)
    upload_json('user_info.json', d)
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("Просмотр данных с приборов"))
    markup.add(types.KeyboardButton("Быстрый доступ"))
    bot.send_message(message.chat.id,
                     text=f"Здравствуйте, коллега. Этот бот создан для просмотра данных с этих приборов: "
                          f"{', '.join(list_devices)}",
                     reply_markup=markup)


def work_with_latest_file(user_id):
    user_info_open = load_json('user_info.json')
    device = user_info_open[user_id]['device']
    last_record_file = f"{main_path}/{device}/{max(list(filter(lambda x: '.csv' in x, os.listdir(f'{main_path}/{device}'))))}"
    file_name = pd.read_csv(preprocessing_one_file(last_record_file))
    max_date = str(file_name[load_json('config_devices.json')[device]['time_cols']].max()).split()[0]
    devices_tech_info_open = load_json('devices_tech_info.json')
    devices_tech_info_open[device] = {'last_record_file': last_record_file}
    user_info_open[user_id]['last_record_date'] = max_date
    upload_json('user_info.json', user_info_open)
    upload_json('devices_tech_info.json', devices_tech_info_open)


def work_with_first_file(user_id):
    device = load_json('user_info.json')[user_id]['device']
    first_record_file = min(list(filter(lambda x: '.csv' in x, os.listdir(f'proc_data/{device}'))))
    df = pd.read_csv(f"proc_data/{device}/{first_record_file}")
    time_col = load_json('config_devices.json')[device]['time_cols']
    devices_tech_info_open = load_json('devices_tech_info.json')
    devices_tech_info_open[device]['first_record_date'] = str(df[time_col].min()).split()[0]
    upload_json('devices_tech_info.json', devices_tech_info_open)


@bot.message_handler(func=lambda
        message: message.text in ['Просмотр данных с приборов', 'Быстрый доступ',
                                  'Настроить быстрый доступ'] + list_devices)
def choose_device(message):
    if message.text in ['Просмотр данных с приборов', 'Настроить быстрый доступ']:
        if message.text == 'Настроить быстрый доступ':
            d = load_json('user_info.json').copy()
            d[str(message.from_user.id)]['update_quick_access'] = True
            upload_json('user_info.json', d)
        markup = types.ReplyKeyboardMarkup(row_width=1)
        markup.add(*list(map(lambda x: types.KeyboardButton(x), list_devices)))
        bot.send_message(message.chat.id, "Выберите прибор", reply_markup=markup)

    elif message.text in list_devices:
        user_info_open = load_json('user_info.json')
        user_info_open[str(message.from_user.id)]['device'] = message.text
        upload_json('user_info.json', user_info_open)
        work_with_latest_file(str(message.from_user.id))
        work_with_first_file(str(message.from_user.id))
        choose_time_delay(message)
    elif message.text == 'Быстрый доступ':
        markup = types.ReplyKeyboardMarkup(row_width=1)
        markup.add('Настроить быстрый доступ')
        if 'quick_access' in load_json('user_info.json')[str(message.from_user.id)].keys():
            markup.add('Отрисовка графика')
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text == 'Отрисовка графика')
def logic_draw_plot(message):
    concat_files(message)


@bot.message_handler(func=lambda message: message.text in ['2 дня', '7 дней', '14 дней',
                                                           '31 день'] or message.text == 'Свой временной промежуток')
def choose_time_delay(message):
    if message.text in ['2 дня', '7 дней', '14 дней', '31 день']:
        delay = 2 if message.text == '2 дня' else 7 if message.text == '7 дней' else 14 if message.text == '14 дней' else 31
        user_info_open = load_json('user_info.json')
        end_record_date = user_info_open[str(message.from_user.id)]['last_record_date']
        begin_record_date = (datetime.strptime(end_record_date, '%Y-%m-%d') - timedelta(days=delay)).strftime(
            '%Y-%m-%d')
        user_info_open[str(message.from_user.id)]['begin_record_date'] = str(begin_record_date).split()[0]
        upload_json('user_info.json', user_info_open)
        choose_columns(message)
    elif message.text == 'Свой временной промежуток':
        choose_not_default_start_date(message)
    else:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(types.KeyboardButton('2 дня'), types.KeyboardButton('7 дней'))
        markup.add(types.KeyboardButton('14 дней'), types.KeyboardButton('31 день'))
        markup.add(types.KeyboardButton('Свой временной промежуток'))
        bot.send_message(message.chat.id, "Выберите временной промежуток", reply_markup=markup)


def choose_not_default_start_date(message):
    devices_tech_info_open = load_json('devices_tech_info.json')
    user_info_open = load_json('user_info.json')[str(message.from_user.id)]
    device = user_info_open['device']
    first_record_date = devices_tech_info_open[device]['first_record_date']
    first_record_date = datetime.strptime(first_record_date, "%Y-%m-%d").strftime("%d.%m.%Y")
    last_record_date = user_info_open['last_record_date']
    last_record_date = datetime.strptime(last_record_date, "%Y-%m-%d").strftime("%d.%m.%Y")
    bot.send_message(message.chat.id, f"Данные досупны с {first_record_date} по {last_record_date}")
    msg = bot.send_message(message.chat.id, "Дата начала отрезка данных (в формате 'день.месяц.год')",
                           reply_markup=types.ReplyKeyboardRemove())
    bot.register_next_step_handler(msg, begin_record_date_choose)


def begin_record_date_choose(message):
    devices_tech_info_open = load_json('devices_tech_info.json')
    user_info_open = load_json('user_info.json')
    device = user_info_open[str(message.from_user.id)]['device']
    first_record_date = devices_tech_info_open[device]['first_record_date']
    first_record_date = datetime.strptime(first_record_date, "%Y-%m-%d").date()
    last_record_date = user_info_open[str(message.from_user.id)]['last_record_date']
    last_record_date = datetime.strptime(last_record_date, "%Y-%m-%d").date()
    try:
        begin_record_date = datetime.strptime(message.text, "%d.%m.%Y").date()
        if not last_record_date >= begin_record_date >= first_record_date:
            raise ValueError
        user_info_open[str(message.from_user.id)]['begin_record_date'] = str(begin_record_date).split()[0]
        upload_json('user_info.json', user_info_open)
        choose_not_default_finish_date(message)
    except ValueError:
        bot.send_message(message.chat.id, "Введена некорректная дата")
        choose_not_default_start_date(message)


def choose_not_default_finish_date(message):
    msg = bot.send_message(message.chat.id, "Дата конца отрезка данных (в формате 'день.месяц.год')")
    bot.register_next_step_handler(msg, end_record_date_choose)


def end_record_date_choose(message):
    devices_tech_info_open = load_json('devices_tech_info.json')
    user_info_open = load_json('user_info.json')
    device = user_info_open[str(message.from_user.id)]['device']
    begin_record_date = user_info_open[str(message.from_user.id)]['begin_record_date']
    begin_record_date = datetime.strptime(begin_record_date, "%Y-%m-%d").date()
    last_record_date = user_info_open[str(message.from_user.id)]['last_record_date']
    last_record_date = datetime.strptime(last_record_date, "%Y-%m-%d").date()

    try:
        end_record_date = datetime.strptime(message.text, "%d.%m.%Y").date()
        if not (last_record_date >= end_record_date >= begin_record_date):
            raise ValueError
        user_info_open[str(message.from_user.id)]['last_record_date'] = str(end_record_date).split()[0]
        upload_json('user_info.json', user_info_open)
        choose_columns(message)
    except ValueError:
        bot.send_message(message.chat.id, "Введена некорректная дата")
        choose_not_default_finish_date(message)


def draw_inline_keyboard(selected_columns, ava_col):
    markup = types.InlineKeyboardMarkup(row_width=1)
    for i in ava_col:
        emoji = ' ✔️' if i in selected_columns else ' ❌'
        markup.add(types.InlineKeyboardButton(str(i) + emoji, callback_data=f'feature_{str(i)}'))
    markup.add(types.InlineKeyboardButton('Выбрано', callback_data='next'))
    return markup


@bot.callback_query_handler(func=lambda call: True)
def choose_columns(call):
    if isinstance(call, CallbackQuery):
        text = call.data
    else:
        text = call.text
    if text.startswith('feature'):
        feature = "_".join(call.data.split('feature')[1].split("_")[1::])
        user_info_open = load_json('user_info.json')
        selected_features = user_info_open[str(call.from_user.id)]['selected_columns']
        if feature in selected_features:
            selected_features.remove(feature)
            bot.answer_callback_query(call.id, 'Вы убрали столбец ' + feature)
        else:
            selected_features.append(feature)
            bot.answer_callback_query(call.id, 'Вы добавили столбец ' + feature)
        user_info_open[str(call.from_user.id)]['selected_columns'] = selected_features
        upload_json('user_info.json', user_info_open)
        bot.answer_callback_query(call.id, 'Вы выбрали Фичу ' + feature)
        selected_columns = user_info_open[str(call.from_user.id)]['selected_columns']
        ava_col = load_json('config_devices.json')[user_info_open[str(call.from_user.id)]['device']]['cols']
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Нажми",
                              reply_markup=draw_inline_keyboard(selected_columns, ava_col))

    elif text == 'next':
        if len(load_json('user_info.json')[str(call.from_user.id)]['selected_columns']) != 0:
            concat_files(call)
        else:
            bot.answer_callback_query(call.id, 'Ни один параметр не выбран!')
    else:
        user_info_open = load_json('user_info.json')
        ava_col = load_json('config_devices.json')[user_info_open[str(call.from_user.id)]['device']]['cols']
        if 'selected_columns' not in user_info_open[str(call.from_user.id)].keys():
            user_info_open[str(call.from_user.id)]['selected_columns'] = ava_col
        upload_json('user_info.json', user_info_open)
        selected_columns = user_info_open[str(call.from_user.id)]['selected_columns']
        bot.send_message(call.chat.id, 'Столбцы для выбора:',
                         reply_markup=draw_inline_keyboard(selected_columns, ava_col))


def concat_files(message):
    if isinstance(message, CallbackQuery):
        text = message.data
    else:
        text = message.text

    user_info_open = load_json('user_info.json')
    if user_info_open[str(message.from_user.id)]['update_quick_access']:
        user_info_open[str(message.from_user.id)]['quick_access'] = user_info_open[str(message.from_user.id)].copy()
        user_info_open[str(message.from_user.id)]['update_quick_access'] = False
        upload_json('user_info.json', user_info_open)
        bot.send_message(str(message.from_user.id), 'Параметры для быстрого доступа выбраны. ')
    if text == 'Отрисовка графика':
        user_info_open = load_json('user_info.json')
        user_id = user_info_open[str(message.from_user.id)]['quick_access']
    else:
        user_info_open = load_json('user_info.json')
        user_id = user_info_open[str(message.from_user.id)]
    device = user_id['device']
    begin_record_date = datetime.strptime(user_id['begin_record_date'], '%Y-%m-%d')
    end_record_date = datetime.strptime(user_id['last_record_date'], '%Y-%m-%d')
    current_date, combined_data = begin_record_date, pd.DataFrame()
    while current_date <= end_record_date + timedelta(days=32):
        try:
            data = pd.read_csv(f"proc_data/{device}/{current_date.strftime('%Y_%m')}.csv")
            combined_data = pd.concat([combined_data, data], ignore_index=True)
            current_date += timedelta(days=29)
        except FileNotFoundError:
            current_date += timedelta(days=29)
    begin_record_date = pd.to_datetime(begin_record_date)
    end_record_date = pd.to_datetime(end_record_date)
    device_dict = load_json('config_devices.json')[device]
    time_col = device_dict['time_cols']
    combined_data[time_col] = pd.to_datetime(combined_data[time_col], format="%Y-%m-%d %H:%M:%S")
    combined_data = combined_data[
        (combined_data[time_col] >= begin_record_date) & (combined_data[time_col] <= end_record_date)]
    combined_data.set_index(time_col, inplace=True)
    combined_data = combined_data.replace(',', '.', regex=True).astype(float)
    if (end_record_date - begin_record_date).days > 2 and len(combined_data) >= 500:
        combined_data = combined_data.resample('60min').mean()
    cols_to_draw = user_id['selected_columns']
    combined_data.reset_index(inplace=True)
    fig = go.Figure()
    fig.update_layout(
        title=str(device),
        xaxis=dict(title="Time"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=True
    )
    fig.update_traces(line={'width': 2})
    fig.update_xaxes(gridcolor='grey',
                     showline=True,
                     linewidth=1,
                     linecolor='black',
                     mirror=True,
                     tickformat='%d.%m.%Y')
    fig.update_yaxes(gridcolor='grey',
                     showline=True,
                     linewidth=1,
                     linecolor='black',
                     mirror=True)
    for col in cols_to_draw:
        fig.add_trace(go.Scatter(x=combined_data[time_col], y=combined_data[col],
                                 mode='lines',
                                 name=col,
                                 line=go.scatter.Line(
                                     color=device_dict['color_dict'][col])))
    fig.write_image(f"graphs_photo/{str(message.from_user.id)}.png")
    bot.send_photo(str(message.from_user.id), photo=open(f"graphs_photo/{str(message.from_user.id)}.png", 'rb'))
    plt.close()


bot.polling(none_stop=True)

"""def choose_columns(message):
    user_info_open = json.load(open('user_info.json', 'r'))
    markup = types.InlineKeyboardMarkup(row_width=1)
    device = user_info_open[str(message.from_user.id)]['device']
    ava_col = json.load(open('config_devices.json', 'r'))[device]['cols']
    for i in ava_col:
        markup.add(types.InlineKeyboardButton(str(i), callback_data=str(i)))
    next = types.InlineKeyboardButton('Выбрано', callback_data='next')
    back = types.InlineKeyboardButton('Обратно', callback_data='back')
    markup.add(next, back)
    bot.send_message(message.chat.id, 'Столбцы для выбора:', reply_markup=markup)
"""

"""
Сохранение цветов к столбцам
f = json.load(open('config_devices.json', 'r'))
colors = px.colors.qualitative.Alphabet
for i in f.keys():
    f[i]['color_dict'] = {}
    for j in range(len(f[i]['cols'])):
        f[i]['color_dict'][f[i]['cols'][j]] = colors[j]
"""
