import operator
import matplotlib
import logging
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def autolabel2(rects, ax):
    # Get y-axis height to calculate label position from.
    (y_bottom, y_top) = ax.get_xlim()
    y_width = y_top - y_bottom

    for rect in rects:
        # Fraction of axis height taken up by this barchart
        p_width  = rect.get_width() / y_width

        # If we can fit the label above the column, do that;
        # otherwise, put it inside the column.
        if p_width > 0.95: # arbitrary; 95% looked good to me.
            label_position = y_top - (y_width * 0.05)
        else:
            label_position = rect.get_width() + 2

        #we divide 2.6 to also include the font size
        ax.text(label_position, rect.get_y() + rect.get_height()/2.6,
                '%.1f' % rect.get_width(),
                ha='center', va='bottom', bbox=dict(facecolor='white', alpha=0.5))

def make_barchart(file_name, data):
    """
    make simple barchart for inclusive losses
    """
    logging.debug("making barchart with data: %s" % (data))
    fig, ax = plt.subplots()

    ax.set_title('Inclusive Luminosity Losses')
    ax.set_xlabel(r"Luminosity Loss [pb$^{-1}$]")
    # we reverse whatever we have
    # so biggest is plotted last (in top)
    rects = plt.barh(range(len(data)), [el[1] for el in data], align='center')
    plt.yticks(range(len(data)), [el[0] for el in data])

    __labels = []
    for el in reversed(data):
        __labels.append("%s (%1.2f)" % (el[0], el[1]))
    # autolabel2 add labels/values to end of barchart
    #autolabel2(rects, ax)

    ax.legend(rects, __labels, bbox_to_anchor=(1.05, 1), loc=2,
            fontsize=10)#, handlelength=0.0, handletextpad=0.0)
            # TO-DO: uncomment once matplotlib is updated to 2 in cmssw

    fig.savefig(file_name, bbox_inches="tight")
    plt.close()

def make_pizzachart(f_name, data):
    """
    make piechart plot for exclusive losses
    """
    # sort ascending by value
    data = reversed(sorted(data.items(), key=operator.itemgetter(1)))

    # because people want to se subdetectors in same color all the time
    colors_hashmap = {
            "CSC": "#e6194b",
            "DT": "#aa6e28",
            "ECAL": "#808000",
            "ES": "#008080",
            "HCAL": "#000080",
            "Pix": "#3cb44b",
            "RPC": "orange",
            "Strip": "#000000",
            "HLT": "#911eb4",
            "L1t": "#aaffc3",
            "Lumi": "#fabebe",
            "Mixed": "#f58231",
            "Egamma": "#0082c8",
            "JetMET": "#f032e6",
            "Muon": "#808080",
            "Track": "#ffe119",
            "TK_HV": "#46f0f0"
    }

    __pie_labels = []
    __pie_share = []
    __colors = []
    __explode = []
    fig, ax = plt.subplots(figsize=(6, 5))

    ax.set_title('Exclusive Luminosity Losses in /pb')
    for el in data:
        #we do not plot 0 values
        if el[1] > 0.0:
            __pie_labels.append("%s (%1.2f)" % (el[0], el[1]))
            __pie_share.append(el[1])
            __colors.append(colors_hashmap[el[0]])
            # explode piechart parts is value is smaller than 1
            if el[1] < 1.0:
                __explode.append(0.1)
            else:
                __explode.append(0.0)

    pie_collection = ax.pie(__pie_share, explode=__explode, colors=__colors,
            startangle=90)

    # make the piechart section spaces white
    for pie_wedge in pie_collection[0]:
        pie_wedge.set_edgecolor('white')

    # add the legend to right side center.
    plt.legend(pie_collection[0], __pie_labels, bbox_to_anchor=(1, 0.5),
            loc="center right", fontsize=10,
            bbox_transform=fig.transFigure)

    # adjust the plot location to include legend into picture
    plt.subplots_adjust(left=0.1, bottom=0.1, right=0.75)

    fig.savefig(f_name, bbox_inches="tight")
    plt.close()