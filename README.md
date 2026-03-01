# HiFIVE  
**High-Fidelity Vector-Tile Reduction for Interactive Map Exploration**

HiFIVE is a visualization-aware vector-tile reduction framework built on top of **BEAST / DaVinci**.  
It generates **size-bounded Mapbox Vector Tiles (MVT)** while preserving spatial structure, visual density, and attribute diversity, enabling scalable and faithful interactive map exploration of large geospatial datasets.

---

## Key Features

- Tile-level reduction with strict size constraints
- Visualization-aware feature scoring (geometry, pixels, attributes)
- Multiple reduction strategies (LP, greedy, sampling)
- Designed for large-scale Spark-based pipelines
- Compatible with BEAST `vmplot`

---

## Requirements

- Java 8+
- Apache Spark
- BEAST / DaVinci
- *(Optional)* Gurobi Optimizer (for LP-based reduction)

---

## Example: Running HiFIVE with `vmplot`

Below is a complete example command using **LP-based reduction** on the eBird dataset.

```bash
bin/beast vmplot \
  /user/tbaha001/ebird/features.geojson \
  ebird_LP_vmplot_zoom20 \
  levels:0..20 \
  iformat:geojson \
  compact:false \
  reductionThreshold:256000 \
  reductionMethod:lp \
  'sampler:mode=sort;priority=vertices:largest;capacity=cells:1500000'
