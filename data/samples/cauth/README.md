# samples/cauth

## Contenidos:
### [cauths.jsonl](cauths.jsonl)
Documento de ejemplo que reúne toda la información referida a poderes adjudicadores extraida de “contratacion.euskadi.eus” a fecha 2022/06/10.

Es el resultado de ejecutar el script [e_cauths.py](../../../scripts/extractors/e_cauths.py).

### [raw_html_v1_Izenpe.html](raw_html_v1_Izenpe.html)
Página descargada del `cauth` con `cod_perfil = 2`, que corresponde a **Izenpe**.

La estructura HTML de este archivo corresponde a la `cauth_version = v1`, que usa un parser específico.

### [raw_html_v2_GobiernoVasco.html](raw_html_v2_GobiernoVasco.html)
Página descargada del `cauth` con `cod_perfil = 1`, que corresponde al **Gobierno Vasco**.

La estructura HTML de este archivo corresponde a la `cauth_version = v2`, que usa un parser específico.
