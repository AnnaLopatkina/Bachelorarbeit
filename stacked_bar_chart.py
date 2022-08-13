import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_filter_execution_time():
    labels = ['parquet', 'df', 'csv', 'csv.gz', 'virtual_table']
    zero = [0.0, 0.0, 0.04998369216918945, 0.06875057220458984, 0.0]  # 0
    twenty_five = [0.0, 0.0, 0.04689879417419433, 0.06870651245117188, 0.0]  # 25
    fifty = [0.0, 0.0, 0.04687061309814453, 0.07187347412109375, 0.0]  # 50
    seventy_five = [0.0, 0.0, 0.04999237060546875, 0.0656923770904541, 0.0]  # 75
    hundred = [0.0, 0.0, 0.0499690055847168, 0.06564598083496094, 0.0]  # 100
    dict = [zero, twenty_five, fifty, seventy_five, hundred]
    width = 0.35
    fig, ax = plt.subplots()
    ax.bar(labels, zero, width, label='0%', color='#2cf2de')
    ax.bar(labels, twenty_five, width, label='25%', bottom=dict[0], color='#228D92')
    ax.bar(labels, fifty, width, label='50%', bottom=np.array(dict[0]) + np.array(dict[1]), color='#1D5B6C')
    ax.bar(labels, seventy_five, width, label='75%', bottom=np.array(dict[0]) + np.array(dict[1]) + np.array(dict[2]),
           color='#1A3550')
    ax.bar(labels, hundred, width, label='100%',
           bottom=np.array(dict[0]) + np.array(dict[1]) + np.array(dict[2]) + np.array(dict[3]), color='#0A111E')
    ax.set_ylabel('time in seconds')
    ax.set_title('Execution time')
    ax.legend()
    plt.show()


def plot_filter_memory_used():
    labels = ['parquet', 'df', 'csv', 'csv.gz', 'virtual_table']
    zero = [0.17734375, 0.0109375, 0.4234375, 0.06171875, 0.0015625]  # 0
    twenty_five = [0.190625, 0.00625, 0.421875, 0.0796875, 0.0015625]  # 25
    fifty = [0.178125, 0.00546875, 0.553125, 0.0765625, 0.0015625]  # 50
    seventy_five = [0.1921875, 0.00546875, 0.515625, 0.084375, 0.00234375]  # 75
    hundred = [0.18125, 0.00546875, 0.521875, 0.09140625, 0.00703125]  # 100

    dict = [zero, twenty_five, fifty, seventy_five, hundred]
    width = 0.35
    fig, ax = plt.subplots()

    ax.bar(labels, zero, width, label='0%', color='#2cf2de')
    ax.bar(labels, twenty_five, width, label='25%', bottom=dict[0], color='#228D92')
    ax.bar(labels, fifty, width, label='50%', bottom=np.array(dict[0]) + np.array(dict[1]), color='#1D5B6C')
    ax.bar(labels, seventy_five, width, label='75%', bottom=np.array(dict[0]) + np.array(dict[1]) + np.array(dict[2]),
           color='#1A3550')
    ax.bar(labels, hundred, width, label='100%',
           bottom=np.array(dict[0]) + np.array(dict[1]) + np.array(dict[2]) + np.array(dict[3]), color='#0A111E')
    ax.set_ylabel('memory used in MB')
    ax.set_title('Memory usage')
    ax.legend()
    plt.show()


def plot_group_by_execution():
    labels = ['parquet', 'df', 'csv', 'csv.gz', 'virtual_table']
    hundred = [1.6287477970123292, 1.2418577671051025, 6.235804080963135, 11.14533486366272, 0.9422278881072998]  # 100
    seventy_five = [1.312464427947998, 0.9344526767730713, 5.893661165237427, 11.03763313293457,
                    0.6746510982513427]  # 75
    fifty = [0.9718699932098389, 0.578130054473877, 5.558972978591919, 10.360782480239868, 0.32595057487487794]  # 50
    twenty_five = [0.7593693733215332, 0.368256950378418, 5.297574472427368, 10.063259744644165, 0.13077840805053711]  # 25
    zero = [0.6156352996826172, 0.2241452693939209, 5.0755609512329105, 9.909011030197144, 0.0]  # 0
    dict = [zero, twenty_five, fifty, seventy_five, hundred]
    width = 0.35
    fig, ax = plt.subplots()
    ax.bar(labels, zero, width, label='0%', color='#2cf2de')
    ax.bar(labels, twenty_five, width, label='25%', bottom=dict[0], color='#228D92')
    ax.bar(labels, fifty, width, label='50%', bottom=np.array(dict[0]) + np.array(dict[1]), color='#1D5B6C')
    ax.bar(labels, seventy_five, width, label='75%', bottom=np.array(dict[0]) + np.array(dict[1]) + np.array(dict[2]),
           color='#1A3550')
    ax.bar(labels, hundred, width, label='100%',
           bottom=np.array(dict[0]) + np.array(dict[1]) + np.array(dict[2]) + np.array(dict[3]), color='#0A111E')
    ax.set_ylabel('time in seconds')
    ax.set_title('Execution time')
    ax.legend()
    plt.show()


def plot_group_by_memory():
    labels = ['parquet', 'df', 'csv', 'csv.gz', 'virtual_table']
    hundred = [33.015625, -0.975, 0.4546875, -0.165625, 22.96640625]  # 100
    seventy_five = [28.08125, -0.26796875, 0.353125, -0.04921875, 21.26875]  # 75
    fifty = [16.6609375, -0.1375, 0.36953125, -0.07109375, 11.328125]  # 50
    twenty_five = [8.46640625, -0.29453125, 0.39375, -0.0625, 4.1359375]  # 25
    zero = [0.20078125, 0.0296875, 0.0796875, 0.14453125, 0.0046875]  # 0

    dict = [zero, twenty_five, fifty, seventy_five, hundred]
    width = 0.35

    fig, ax = plt.subplots()
    # ax.bar(labels, zero, width, label='0%')
    # ax.bar(labels, zero, width, label='0%', color='#2cf2de')
    ax.bar(labels, twenty_five, width, label='25%', bottom=dict[0], color='#228D92')
    ax.bar(labels, fifty, width, label='50%', bottom=np.array(dict[0]) + np.array(dict[1]), color='#1D5B6C')
    ax.bar(labels, seventy_five, width, label='75%', bottom=np.array(dict[0]) + np.array(dict[1]) + np.array(dict[2]),
           color='#1A3550')
    ax.bar(labels, hundred, width, label='100%',
           bottom=np.array(dict[0]) + np.array(dict[1]) + np.array(dict[2]) + np.array(dict[3]), color='#0A111E')

    ax.set_ylabel('Memory usage in MB')
    ax.set_title('Memory usage')
    ax.legend()
    plt.show()


def pos_and_neg():

    df = pd.DataFrame(index=['parquet', 'df', 'csv', 'csv.gz', 'virtual_table'],
                      data={# '0%': [0.20078125, 0.0296875, 0.0796875, 0.14453125, 0.0046875],
                            '25%': [8.46640625, -0.29453125, 0.39375, -0.0625, 4.1359375],
                            '50%': [16.6609375, -0.1375, 0.36953125, -0.07109375, 11.328125],
                            '75%': [28.08125, -0.26796875, 0.353125, -0.04921875, 21.26875],
                            '100%': [33.015625, -0.975, 0.4546875, -0.165625, 22.96640625]})
    ax = df.plot(kind="bar", stacked=True)
    # df.sum().plot(ax=ax, color="k")
    # df.sum(axis=1).plot(ax=ax, color="k")
    # ax.bar(df.index)
    plt.xticks(rotation='horizontal',  ha='center')
    ax.set_ylabel('memory used in MB')
    ax.set_title('memory used')
    ax.legend()
    plt.show()


def normalize_data(data):
    return data / np.max(data)


def line_diagram():
    # labels = ['parquet', 'df', 'csv', 'csv.gz', 'virtual_table']
    hundred = [1.6287477970123292, 1.2418577671051025, 6.235804080963135, 11.14533486366272, 0.9422278881072998]  # 100
    seventy_five = [1.312464427947998, 0.9344526767730713, 5.893661165237427, 11.03763313293457,
                    0.6746510982513427]  # 75
    fifty = [0.9718699932098389, 0.578130054473877, 5.558972978591919, 10.360782480239868, 0.32595057487487794]  # 50
    twenty_five = [0.7593693733215332, 0.368256950378418, 5.297574472427368, 10.063259744644165,
                   0.13077840805053711]  # 25
    zero = [0.6156352996826172, 0.2241452693939209, 5.0755609512329105, 9.909011030197144, 0.0]  # 0
    dict = np.array([zero, twenty_five, fifty, seventy_five, hundred])
    normalised_data = []
    for i in dict:
        normalised_data.append(normalize_data(i))
    # scaled_dict = normalize_data(dict[0])
    print(normalised_data) #[0.14613718 0.111424   0.55949903 1.         0.08454011]
    fig = plt.figure()
    ax = plt.axes()
    x = np.linspace(0, 10, 1000)
    plt.plot(normalised_data[0][0], normalised_data[0][1], 'blue')
    plt.show()


if __name__ == '__main__':
    plot_group_by_memory()
