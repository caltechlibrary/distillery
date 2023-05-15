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
      <p><a href="{{ archivesspace_url }}">Open the {{ display_string }} record in its archival context</a></p>
      <div class="uv" id="uv" style="width:100%;height:80vh"></div>
      <script>
        const data = {{ iiif_manifest_json }}
        uv = UV.init("uv", data);
      </script>
      <dl>
        {% if dates %}
        <dt>Dates # TODO tests needed for each date situation</dt>
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
        {% if abstract_notes %}
        <dt>Abstract</dt>
        {% for abstract in abstract_notes %}
        <dd>{{ abstract }}</dd>
        {% endfor %}
        {% endif %}
        {% if scopecontent_notes %}
        <dt>Scope and Contents</dt>
        {% for scopecontent in scopecontent_notes %}
        <dd>{{ scopecontent }}</dd>
        {% endfor %}
        {% endif %}
      </dl>
      <div><a href="{{ iiif_manifest_url }}"><img alt="IIIF Manifest" src="https://iiif.io/assets/uploads/logos/logo-iiif-34x30.png"></a></div>
      <article>
        <h2>❓ Development Questions</h2>
        <ul style="list-style-type:none!important">
          <li>Must an archival object and its ancestors be set to published in ArchivesSpace before we create an HTML page for the record?</li>
          <li>For the page title should we use the <code>display_string</code> that includes appended dates or just the <code>title</code> string?</li>
          <li>Any preferences for hierarchy separators? (Currently set to a / character.)</li>
          <li>What should the link text to ArchivesSpace say?</li>
          <li>What format should be used for date and date range display?</li>
          <li>Which note types are available for display?</li>
          <li>How much note content should be displayed? (Multiple notes of each type can be repeated and each note can contain multiple content fields.)</li>
        </ul>
      </article>
    </main>
  </body>
</html>
