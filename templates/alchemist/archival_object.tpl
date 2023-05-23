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
        background-color: revert;
        border: initial;
        border-radius: initial;
      }
      #uv label {
        overflow-wrap: initial;
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
          {% if date.end %}
          <span>{{ date.begin }} to {{ date.end }}</span>{{ "; " if not loop.last else "" }}
          {% elif date.begin %}
          <span>{{ date.begin }}</span>{{ "; " if not loop.last else "" }}
          {% else %}
          <span>{{ date.expression }}</span>{{ "; " if not loop.last else "" }}
          {% endif %}
          {% endfor %}
        </div>
        {% endif %}
        {% if collection %}
        <h2>
          {{ collection }}{% if series %} / {{ series}}{% endif %}{% if subseries %} / {{ subseries}}{% endif %}
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
            options: { rightPanelEnabled: false }
          });
        });
      </script>
      <dl>
        {% if dates %}
        <dt>Dates</dt>
        {% for date in dates %}
        {% if date.end %}
        <dd>{{ date.begin }} to {{ date.end }}</dd>
        {% elif date.begin %}
        <dd>{{ date.begin }}</dd>
        {% else %}
        <dd>{{ date.expression }}</dd>
        {% endif %}
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
      <div><a href="{{ iiif_manifest_url }}"><img alt="IIIF Manifest" src="https://iiif.io/assets/uploads/logos/logo-iiif-34x30.png"></a></div>
    </main>
  </body>
</html>
