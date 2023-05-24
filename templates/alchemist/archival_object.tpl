<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
    <title>{{ title }}</title>
    <script src="https://cdn.jsdelivr.net/npm/universalviewer@4.0.17/dist/umd/UV.min.js"></script>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/universalviewer@4.0.17/dist/uv.min.css">
    <style>
      /* override pico styles for uv elements */
      #uv button {
        width: initial;
      }
      #uv [role="button"] {
        background-color: initial;
        border: initial;
        border-radius: initial;
        cursor: pointer;
        opacity: initial;
        padding: initial;
        pointer-events: initial;
      }
      #uv h2 {
        color: inherit;
      }
      #uv input {
        border-radius: initial;
      }
      #uv .headerPanel .mode label {
        width: auto;
      }
      #uv .headerPanel input {
        background-color: revert;
        border: initial;
        text-align: center;
      }
      #uv .headerPanel .search {
        width: auto;
      }
      #uv .headerPanel .search .btn {
        padding-block-start: 4px;
        padding-block-end: 0;
      }
      #uv label {
        overflow-wrap: initial;
      }
      #uv .overlays {
        display: flex;
        align-items: center;
        justify-content: center;
      }
      #uv .overlay .btn {
        padding: 10px 15px;
      }
      #uv .overlay.settings {
        height: auto;
        position: unset;
      }
      #uv .overlay.settings .scroll {
        height: auto;
        margin-block-end: var(--form-element-spacing-vertical);
      }
      #uv .overlay.settings .setting select {
        height: auto;
        padding: var(--form-element-spacing-vertical) var(--form-element-spacing-horizontal);
        padding-inline-end: calc(var(--form-element-spacing-horizontal) + 1.5rem);
      }
      #uv .overlay.settings .setting input[type="checkbox"] {
        margin-inline-start: var(--outline-width);
      }
      #uv .iiif-gallery-component input[type="range"]::-moz-range-track {
        width: auto;
      }
      /* many elements need font-size adjustments */
      #uv .overlay .heading {
        font-size: 20px;
      }
      #uv .overlay h2 {
        font-size: 18px;
      }
      #uv .iiif-gallery-component .thumb .label,
      #uv .headerPanel .mode label,
      #uv .headerPanel input,
      #uv .headerPanel .total,
      #uv .headerPanel .search .btn,
      #uv .overlay.settings .setting,
      #uv .overlay.settings .setting select,
      #uv .overlay button {
        font-size: 16px;
      }
      #uv .overlay.settings .version,
      #uv .overlay.settings .website {
        font-size: 14px;
      }
    </style>
  </head>
  <body>
    <main class="container">
      <header class="headings">
        <h1>
          {{ title }}
        </h1>
        {% if dates %}
        <div id="dates">
          {% for date in dates %}
          <span>{{ date }}</span>{{ "; " if not loop.last else "" }}
          {% endfor %}
        </div>
        {% endif %}
        {% if collection %}
        <h2>
          {{ collection }}{% if series %} / {{ series }}{% endif %}{% if subseries %} / {{ subseries }}{% endif %}
        </h2>
        {% endif %}
      </header>
      <p><a href="{{ archivesspace_url }}">Open the <cite>{{ title }}</cite> record in its archival context</a></p>
      <div class="uv" id="uv" style="width:100%;height:80vh"></div>
      <script>
        const data = {{ iiif_manifest_json }}
        uv = UV.init("uv", data);
        // override config using an inline json object
        uv.on("configure", function ({ config, cb }) {
          cb({
            options: { rightPanelEnabled: false, termsOfUseEnabled: false },
            modules: {
              footerPanel: { options: { shareEnabled: false } },
              openSeadragonCenterPanel: { options: { requiredStatementEnabled: false } },
            },
          });
        });
      </script>
      <dl>
        <dt>Title</dt>
        <dl>{{ title }}</dl>
        {% if collection %}
        <dt>Collection</dt>
        <dl>{{ collection }}</dl>
        {% endif %}
        {% if series %}
        <dt>Series</dt>
        <dl>{{ series}}</dl>
        {% endif %}
        {% if subseries %}
        <dt>Sub-Series</dt>
        <dl>{{ subseries }}</dl>
        {% endif %}
        {% if dates %}
        <dt>Dates</dt>
        {% for date in dates %}
        <dd>{{ date }}</dd>
        {% endfor %}
        {% endif %}
        {% if extents %}
        <dt>Extents</dt>
        {% for extent in extents %}
        <dd>{{ extent }}</dd>
        {% endfor %}
        {% endif %}
        {% if subjects %}
        <dt>Subjects</dt>
        {% for subject in subjects %}
        <dd>{{ subject }}</dd>
        {% endfor %}
        {% endif %}
        {% if notes %}
        {% for note_label, note_contents in notes.items() if note_contents %}
        <dt>{{ note_label }}</dt>
        {% for note_content in note_contents %}
        <dd>{{ note_content }}</dd>
        {% endfor %}
        {% endfor %}
        {% endif %}
      </dl>
      <div><a href="{{ iiif_manifest_url }}">
        <svg width="32" viewBox="0 0 493.36 441.33" xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="iiif">
          <title id="iiif">IIIF Manifest</title>
          <g transform="matrix(1.3333 0 0 -1.3333 0 441.33)"><g transform="scale(.1)"><path d="m65.242 2178.8 710-263.75-1.25-1900-708.75 261.25v1902.5" fill="#2873ab"/><path d="m804.14 2640.1c81.441-240.91-26.473-436.2-241.04-436.2-214.56 0-454.51 195.29-535.95 436.2-81.434 240.89 26.48 436.18 241.04 436.18 214.57 0 454.51-195.29 535.95-436.18" fill="#2873ab"/><path d="m1678.6 2178.8-710-263.75 1.25-1900 708.75 261.25v1902.5" fill="#ed1d33"/><path d="m935.08 2640.1c-81.437-240.91 26.477-436.2 241.04-436.2 214.56 0 454.51 195.29 535.96 436.2 81.43 240.89-26.48 436.18-241.04 436.18-214.57 0-454.52-195.29-535.96-436.18" fill="#ed1d33"/><path d="m1860.2 2178.8 710-263.75-1.25-1900-708.75 261.25v1902.5" fill="#2873ab"/><path d="m2603.7 2640.1c81.45-240.91-26.47-436.2-241.03-436.2-214.58 0-454.52 195.29-535.96 436.2-81.44 240.89 26.48 436.18 241.03 436.18 214.57 0 454.51-195.29 535.96-436.18" fill="#2873ab"/><path d="m3700.2 3310v-652.5s-230 90-257.5-142.5c-2.5-247.5 0-336.25 0-336.25l257.5 83.75v-572.5l-258.61-92.5v-1335l-706.39-262.5v2360s-15 850 965 950" fill="#ed1d33"/></g></g>
        </svg>
      </a></div>
    </main>
  </body>
</html>
