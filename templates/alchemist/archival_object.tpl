<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
    <title>{{ display_string }}</title>
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
          {{ display_string }}
        </h1>
        {% if collection %}
        <h2>
          {{ collection }}{% if series %} / {{ series}}{% endif %}{% if subseries %} / {{ subseries}}{% endif %}
        </h2>
        {% endif %}
      </header>
      <div class="uv" id="uv" style="width:100%;height:80vh"></div>
      <script>
        const data = {{ iiif_manifest_json }}
        uv = UV.init("uv", data);
      </script>
      <dl>
        <dt>Dates</dt>
        <dd>{{ dates }}</dd>
        <dt>Notes</dt>
        <dd>{{ notes }}</dd>
        <dt>URI</dt>
        <dd>{{ uri }}</dd>
      </dl>
      <div><a href="{{ iiif_manifest_url }}"><img alt="IIIF Manifest" src="https://iiif.io/assets/uploads/logos/logo-iiif-34x30.png"></a></div>
    </main>
  </body>
</html>
