# ESSD figure requirements

This document defines the figure-production requirements for manuscripts submitted to **Earth System Science Data (ESSD)**.
Before creating, modifying, exporting, or reviewing any manuscript figure, read and apply this document.

These rules apply to all maps, line plots, scatter plots, histograms, workflow diagrams, multi-panel figures, legends, color bars, and exported figure files.

---

## 1. Required output for each figure

For every final manuscript figure, create and save the following files:

```text
figures/
├── final/
│   ├── f01.pdf
│   ├── f02.pdf
│   └── ...
├── data/
│   ├── f01_plotting_data.csv
│   ├── f02_plotting_data.csv
│   └── ...
├── scripts/
│   ├── plot_f01.py
│   ├── plot_f02.py
│   └── ...
└── checklists/
    ├── f01_checklist.md
    ├── f02_checklist.md
    └── ...
```

Each figure must have:

* a final exported figure file;
* a reproducible plotting script;
* the plotting data or a clearly documented data subset;
* a checklist documenting compliance with this file.

---

## 2. File naming

Use Arabic numerals in sequential order:

```text
f01.pdf
f02.pdf
f03.pdf
...
```

Do not use informal names such as:

```text
Figure1_final_new.png
map_latest_v3.pdf
test_plot.png
```

For multi-panel figures, combine all panels into a single figure file before submission:

```text
Correct:   f03.pdf
Incorrect: f03a.pdf, f03b.pdf, f03c.pdf
```

---

## 3. Preferred file formats

For line plots, maps, statistical plots, and diagrams, prefer vector formats:

```text
.pdf
.eps
.ps
```

If vector output is not possible, use a non-lossy bitmap format:

```text
.png
.tif
```

Use `.jpg` only for photographs. Do not use `.jpg` for line plots, maps, diagrams, or figures with sharp edges.

Do not convert a low-quality `.jpg` back to `.png` and use it as a final figure.

---

## 4. Resolution, size, and file size

Minimum requirements:

* Bitmap figures must be at least **300 dpi**.
* Figure width must be at least **8 cm**.
* A single `.pdf` figure should preferably be **< 2 MB**.
* A non-PDF figure file should be **< 5 MB**.
* The total size of all main submission files, excluding supplements, should be kept below **30 MB**.

When exporting, balance visual quality and file size. Do not reduce quality so much that text, markers, coastlines, or thin lines become unclear.

---

## 5. Color and accessibility

All figures must use color schemes that remain interpretable for readers with color-vision deficiency.

### 5.1 General rules

* Do not rely only on red–green contrast.
* Avoid rainbow or `jet` color maps.
* Use colorblind-safe categorical palettes.
* For continuous variables, prefer perceptually uniform color maps.
* When categories are important, use color plus another visual encoding, such as marker shape, line type, border, fill, or transparency.

### 5.2 Recommended categorical palette

Use the Okabe–Ito colorblind-safe palette when appropriate:

```python
OKABE_ITO = {
    "black": "#000000",
    "orange": "#E69F00",
    "sky_blue": "#56B4E9",
    "bluish_green": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddish_purple": "#CC79A7",
}
```

### 5.3 Recommended continuous color maps

Use one of the following for continuous variables:

```text
viridis
cividis
plasma
magma
```

Avoid:

```text
jet
rainbow
nipy_spectral
```

### 5.4 Coblis check

After exporting the final figure, check the figure using **Coblis – Color Blindness Simulator** or an equivalent color-vision-deficiency simulator.

The figure must remain interpretable under common color-vision-deficiency simulations. If categories become difficult to distinguish, revise the color palette and/or add marker, line-style, or fill-style differences.

Record the result in the figure checklist.

---

## 6. Fonts

Use only one sans-serif font family throughout the figure.

Recommended font families:

```text
Arial
Helvetica
DejaVu Sans
Liberation Sans
```

Do not mix multiple font families in the same figure.

Avoid unnecessary use of bold, italic, and bold italic, because they count as separate font variants and increase font complexity.

All text should be generated directly by the plotting script. Do not add labels later using text boxes in PowerPoint, Word, Illustrator, or other editing software unless absolutely necessary.

For vector outputs, fonts must be embedded.

Recommended Matplotlib settings:

```python
import matplotlib as mpl

mpl.rcParams["font.family"] = "Arial"
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42
mpl.rcParams["svg.fonttype"] = "none"
```

If Arial is unavailable on the system, use another single sans-serif font consistently.

---

## 7. Hidden objects and vector cleanliness

For `.pdf` and `.eps` figures:

* do not include hidden objects;
* do not include invisible layers;
* do not include unnecessary text boxes;
* do not leave unused axes outside the visible plotting area;
* avoid excessive numbers of vector objects when a rasterized layer would be more efficient.

For dense scatter maps or very large point clouds, consider rasterizing only the dense data layer while keeping text, axes, legends, and coastlines as vector elements.

Example:

```python
ax.scatter(x, y, s=2, rasterized=True)
```

---

## 8. Legends and symbol explanation

The legend must be part of the figure itself.

All visual encodings must be explained inside the figure, including:

* colors;
* marker shapes;
* line styles;
* line widths;
* fill styles;
* point sizes;
* transparency;
* hatching;
* color bars.

Do not rely on the figure caption to explain visual symbols such as “dashed line”, “green open circles”, or “large blue points”.

If point size represents a variable such as basin area, record count, sample size, or drainage area, include representative point-size examples in the legend.

The legend should be concise and must not obscure important data.

---

## 9. Multi-panel figures

For multi-panel figures:

* combine all panels into one final file;
* use lowercase panel labels with parentheses;
* use `(a)`, `(b)`, `(c)`, etc.;
* do not use `A`, `B`, `C`;
* do not use `a)`, `b)`, `c)`;
* place panel labels consistently, usually at the upper-left corner of each panel;
* use consistent axis labels, fonts, and spacing across panels.

Panel labels should be clearly visible but not visually dominant.

---

## 10. Text style inside figures

Use sentence case for titles, axis labels, legends, and color-bar labels.

Correct:

```text
Suspended sediment concentration
Daily records
Basin area
```

Avoid title case unless required for proper nouns:

```text
Suspended Sediment Concentration
Daily Records
Basin Area
```

Keep figure text concise. Avoid long sentences inside figures.

---

## 11. Units, numbers, ranges, and coordinates

Follow ESSD formatting rules for all figure text.

### 11.1 Ranges

Use an en dash with no spaces:

```text
1–10
Jan–Feb
1912–2025
```

Do not use:

```text
1 - 10
1-10
Jan - Feb
```

### 11.2 Coordinates

Use degree symbols and direction letters with a space:

```text
30° N
25° E
10° S
120° W
```

### 11.3 Numbers and units

Always include a space between numbers and units:

```text
1 %
1 m
300 km
24 h
```

Do not write:

```text
1%
1m
300km
24hr
```

### 11.4 Unit formatting

Use exponent-style units:

```text
m³ s⁻¹
mg L⁻¹
t d⁻¹
W m⁻²
km²
```

For this sediment dataset, use the following units consistently:

```text
Q   = m³ s⁻¹
SSC = mg L⁻¹
SSL = t d⁻¹
```

Use:

```text
h
km
m
```

Do not use:

```text
hr
kms
meters
```

---

## 12. Figure captions

Do not place the full figure caption inside the figure file.

The figure file should contain only:

* axes;
* labels;
* legends;
* color bars;
* panel labels;
* minimal annotations needed to understand the figure.

Figure captions must be written in the manuscript text file, not embedded in the image.

Any uncommon abbreviation used in the figure must be defined either in the figure legend or in the manuscript caption.

---

## 13. Copyright and figure reuse

For figures created entirely from this study’s data and code, no additional reuse statement is needed beyond the manuscript license.

If a figure is taken directly from another publication:

* confirm that reuse is allowed;
* obtain permission if required;
* cite the original source in the manuscript and figure caption;
* include any required license statement.

If a figure is adapted from another publication:

* cite the original source;
* state clearly that the figure is adapted;
* include wording such as `adapted from Smith et al. (2014)` in the caption;
* confirm that the license allows adaptation.

Do not reuse copyrighted figures, maps, or graphical elements unless reuse rights are clear.

---

## 14. Plotting data and reproducibility

Every final figure must be reproducible from saved plotting data and a plotting script.

The plotting script must explicitly define:

* input data path;
* output figure path;
* figure size;
* dpi;
* color palette;
* marker mapping;
* line-style mapping;
* category ordering;
* variable units;
* filtering rules;
* thresholds;
* statistical summaries shown in the figure.

The plotting data should be saved in a reusable format, such as:

```text
.csv
.parquet
.nc
.gpkg
.geojson
```

If the plotting data are too large, save a documented subset or provide a README explaining how to regenerate the plotting data.

---

## 15. Project-specific figure rules for the sediment reference dataset

### 15.1 Temporal-resolution maps

When showing spatial distributions by temporal resolution, clearly distinguish:

```text
daily
monthly
annual
climatology
```

Use colorblind-safe colors and, where useful, different marker shapes.

Do not rely only on color if categories overlap spatially.

### 15.2 Basin matching status

When showing basin matching status, clearly distinguish:

```text
resolved
unresolved
other / unknown
```

Use both color and marker or border differences when possible.

### 15.3 Satellite validation sources

When showing satellite validation layers, clearly distinguish:

```text
RiverSed
GSED
Dethier
```

Use colorblind-safe colors and, where possible, different marker shapes or edge styles.

### 15.4 Continuous variables

For continuous variables such as:

```text
basin area
upstream drainage area
record count
Q
SSC
SSL
time-series length
```

use a perceptually uniform color map such as:

```text
viridis
cividis
plasma
magma
```

If using point size to represent a continuous variable, include a point-size legend with representative values.

### 15.5 Maps

For maps:

* use WGS84 coordinates unless another projection is explicitly required;
* label longitude and latitude clearly;
* show coordinates using ESSD formatting;
* avoid overly dense labels;
* make coastlines, rivers, and station points visually separable;
* ensure the map remains readable in grayscale and color-vision-deficiency simulations.

---

## 16. Recommended Matplotlib export template

Use this template or an equivalent configuration when creating figures in Python.

```python
from pathlib import Path
import matplotlib as mpl
import matplotlib.pyplot as plt

# Font and vector settings
mpl.rcParams["font.family"] = "Arial"
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42
mpl.rcParams["axes.unicode_minus"] = False

# Figure size
# ESSD requires width >= 8 cm.
# Convert cm to inches: inch = cm / 2.54
width_cm = 18.0
height_cm = 12.0
figsize = (width_cm / 2.54, height_cm / 2.54)

fig, ax = plt.subplots(figsize=figsize)

# Plot here
# ax.plot(...)
# ax.scatter(...)

ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")

# Use clear legend
ax.legend(frameon=True)

# Save
out = Path("figures/final/f01.pdf")
out.parent.mkdir(parents=True, exist_ok=True)

fig.savefig(
    out,
    format="pdf",
    dpi=300,
    bbox_inches="tight",
    metadata={"Creator": "Python Matplotlib"},
)

plt.close(fig)
```

If exporting PNG:

```python
fig.savefig(
    "figures/final/f01.png",
    format="png",
    dpi=300,
    bbox_inches="tight",
)
```

---

## 17. Required checklist for each figure

Create a checklist file for each final figure:

```text
figures/checklists/f01_checklist.md
```

Use the following template.

```markdown
# Figure checklist: f01

## Basic information

- Figure file:
- Plotting script:
- Plotting data:
- Date exported:
- Figure type:
- Single-panel or multi-panel:

## File format and size

- Final format:
- DPI:
- Width:
- Height:
- File size:
- PDF < 2 MB:
- Non-PDF < 5 MB:
- Width >= 8 cm:

## Color and accessibility

- Colorblind-safe palette used:
- Continuous color map, if applicable:
- Coblis or equivalent check completed:
- Figure remains interpretable under color-vision-deficiency simulation:
- Categories are distinguished by more than color when needed:

## Font and text

- Single font family used:
- Font family:
- Fonts embedded in vector file:
- No unnecessary bold/italic variants:
- No hidden text boxes or extra layers:
- Sentence case used:

## Legend and symbols

- Legend included inside figure:
- All colors explained:
- All markers explained:
- All line styles explained:
- Point sizes explained, if applicable:
- Color bar included and labeled, if applicable:
- Legend does not obscure data:

## ESSD formatting

- Panel labels use `(a)`, `(b)`, etc.:
- Ranges use en dash with no spaces:
- Coordinates use degree symbol and direction spacing:
- Numbers and units have a space:
- Units use exponent format:
- h, km, and m abbreviations used correctly:

## Reproducibility

- Plotting data saved:
- Plotting script saved:
- Input paths documented:
- Filtering rules documented:
- Color and marker mappings defined in code:
- Figure can be regenerated from saved files:

## Copyright

- Figure fully generated from study data and code:
- External figure or basemap used:
- Reuse permission checked, if applicable:
- Source cited in caption, if applicable:

## Notes

-
```

---

## 18. Final pre-submission check

Before submission, check that:

* all figures are numbered sequentially as `f01`, `f02`, `f03`, etc.;
* every figure has one final file;
* multi-panel figures are merged into one file;
* all figures meet size, dpi, and width requirements;
* all figures use colorblind-safe visual design;
* all figure legends explain visual encodings;
* all fonts are consistent and embedded where needed;
* all figure captions are in the manuscript text, not inside image files;
* all plotting data and scripts are archived for reproducibility.

