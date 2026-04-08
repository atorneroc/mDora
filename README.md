"# mDora — Sistema de Gestión de Incidentes con Métricas DORA

Sistema IssueOps para capturar, procesar y medir incidentes directamente desde GitHub Issues, calculando automáticamente el **MTTR (Mean Time To Recovery)**.

> No requiere bases de datos externas. Todo se almacena como JSON versionado dentro del repositorio.

---

## Cómo funciona

### 1. Crear un incidente

1. Ve a la pestaña **Issues** del repositorio.
2. Haz clic en **New Issue** y selecciona la plantilla **🚨 Reporte de Incidente**.
3. Completa los campos del formulario:
   - **Fecha y hora de inicio** (formato `YYYY-MM-DD HH:MM`, ejemplo: `2026-04-08 09:30`).
   - **Tipo de error**, **Severidad**, **Solicitante**, **Servicio afectado** y **Descripción**.
   - *(Opcional)* **Fecha de solución** si el incidente ya fue resuelto.
4. Envía el issue. El workflow automáticamente:
   - Valida el formato de la fecha.
   - Etiqueta el issue con la severidad y estado (`open` / `resolved`).
   - Guarda el incidente en `data/incidents.json`.
   - Si incluiste fecha de solución, calcula el MTTR y cierra el issue.
   - Comenta un resumen con la tabla de incidentes recientes.

### 2. Resolver un incidente abierto

Comenta en el issue del incidente con el comando:

```
/resolver YYYY-MM-DD HH:MM
```

**Ejemplo:**

```
/resolver 2026-04-08 11:45
```

El workflow automáticamente:
- Actualiza la fecha de solución en el JSON.
- Calcula el **MTTR** (diferencia entre inicio y solución).
- Cambia el estado a `resuelto`, reemplaza la label `open` por `resolved` y cierra el issue.
- Comenta un resumen con las métricas DORA.

---

## Estructura de archivos

| Archivo | Propósito |
|---------|-----------|
| `.github/ISSUE_TEMPLATE/incident-report.yml` | Formulario de Issue para reportar incidentes. Define los campos y asigna la label `incident` automáticamente. |
| `.github/workflows/process-incident.yml` | Workflow que procesa incidentes: los registra al abrir un issue y los resuelve cuando se comenta `/resolver`. |
| `data/incidents.json` | Almacén JSON versionado con todos los incidentes registrados. Incluye MTTR calculado para incidentes resueltos. |

---

## Configuración requerida

Este sistema solo necesita que habilites los permisos de escritura para GitHub Actions:

1. Ve a **Settings → Actions → General** en tu repositorio.
2. En la sección **Workflow permissions**, selecciona **Read and write permissions**.
3. Guarda los cambios.

No se requiere ningún token adicional ni secreto personalizado. Los workflows usan el `GITHUB_TOKEN` predeterminado con permisos `contents: write` e `issues: write`.

---

## Métricas DORA

El sistema calcula automáticamente:

- **MTTR por incidente:** diferencia en minutos/horas entre `fecha_inicio` y `fecha_solucion`.
- **MTTR promedio global:** promedio de todos los incidentes resueltos.

Estas métricas se reportan como comentarios en cada issue y se almacenan en `data/incidents.json` para análisis posterior.

---

## Ejemplo de registro en `data/incidents.json`

```json
{
  "id": 1,
  "title": "[INCIDENTE] Caída del servidor de producción",
  "fecha_inicio": "2026-04-08T09:30:00",
  "fecha_solucion": "2026-04-08T11:45:00",
  "mttr_minutes": 135,
  "mttr_hours": 2.25,
  "tipo_error": "Infraestructura",
  "severidad": "P1 - Crítico",
  "solicitante": "usuario123",
  "servicio_afectado": "Proyecto A",
  "estado": "resuelto",
  "created_at": "2026-04-08T09:40:00Z",
  "issue_url": "https://github.com/org/repo/issues/1"
}
```

---

## Tecnologías utilizadas

- [GitHub Issue Forms](https://docs.github.com/en/communities/using-templates-to-encourage-useful-issues-and-pull-requests/syntax-for-issue-forms)
- [stefanbuck/github-issue-parser@v3](https://github.com/stefanbuck/github-issue-parser)
- [actions/github-script@v7](https://github.com/actions/github-script)" 
