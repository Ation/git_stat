# https://docs.bokeh.org/en/latest/docs/gallery/range_tool.html
# https://docs.bokeh.org/en/latest/docs/user_guide/tools.html?highlight=hovering
# https://docs.bokeh.org/en/latest/docs/reference/palettes.html?highlight=inferno256#large-palettes
# https://github.com/bokeh/bokeh/blob/2.2.3/examples/howto/Hover%20callback.ipynb



# https://docs.bokeh.org/en/latest/docs/user_guide/interaction/legends.html

from bokeh.plotting import figure, output_file, show
from bokeh.models.tools import HoverTool

# prepare some data
x = [1, 2, 3, 4, 5]
y = [6, 7, 2, 4, 5]

# output to static HTML file
output_file("lines.html")

# create a new plot with a title and axis labels

p = figure(
    y_range=(0, 10),
    toolbar_location=None,
    tools=[HoverTool(mode='vline', tooltips="Data point @x has the value @y")],
    sizing_mode="stretch_width",
    max_width=500,
    plot_height=250,
)

# add a line renderer with legend and line thickness
p.line(x, y, legend_label="Temp.", line_width=2)

# show the results
show(p)


# HoverTool(
#     tooltips=[
#         ( 'date',   '@date{%F}'            ),
#         ( 'close',  '$@{adj close}{%0.2f}' ), # use @{ } for field names with spaces
#         ( 'volume', '@volume{0.00 a}'      ),
#     ],

#     formatters={
#         '@date'        : 'datetime', # use 'datetime' formatter for '@date' field
#         '@{adj close}' : 'printf',   # use 'printf' formatter for '@{adj close}' field
#                                      # use default 'numeral' formatter for other fields
#     },

#     # display a tooltip whenever the cursor is vertically in line with a glyph
#     mode='vline'
# )
