# AGENTS.md

## ESSD figure-production rule

Before creating, modifying, exporting, or reviewing any manuscript figure, read and follow:

`docs/essd_figure_requirements.md`

This requirement applies to all plotting scripts, maps, multi-panel figures, legends, color schemes, exported figure files, and plotting data.

For every final figure, ensure that:

* the figure follows the ESSD formatting rules in `docs/essd_figure_requirements.md`;
* the output file name is specified by the user for each figure; do not auto-assign ESSD-style sequence names such as `f01.pdf`, `f02.pdf`, or `f03.pdf` unless the user explicitly requests that naming scheme;
* the preferred output format is vector `.pdf` or `.eps` when possible;
* bitmap outputs are at least 300 dpi;
* the figure width is at least 8 cm;
* all visible figure text uses a font size of at least 7 pt, including inset axes, tick labels, legend notes, annotations, and point-size example labels;
* color palettes are color-vision-deficiency friendly;
* all symbols, colors, line types, markers, and point-size meanings are explained inside the figure legend;
* multi-panel labels use lowercase letters with parentheses, such as `(a)`, `(b)`, `(c)`;
* all units, coordinates, ranges, and abbreviations follow the ESSD formatting rules;
* plotting data and the plotting script are saved together with the final figure;
* a figure checklist is created or updated after export.

After each figure export, create or update a checklist file under:

`figures/checklists/`

The checklist must record file name, format, dpi, size, dimensions, font consistency, font embedding status, colorblind-safe status, legend completeness, and plotting-data availability.
