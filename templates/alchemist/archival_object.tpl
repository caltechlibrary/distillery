<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
    <title>{display_string}</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/universalviewer@4.0.0/dist/uv.css">
    <script src="https://cdn.jsdelivr.net/npm/universalviewer@4.0.0/dist/umd/UV.js"></script>
  </head>
  <body>
    <main class="container">
      <header class="headings">
        <h1>
          {display_string}
        </h1>
        <h2>
          TODO: add ancestry
        </h2>
      </header>
      <div class="uv" id="uv" style="width:924px;height:668px"></div>
      <script>
        const data = {iiif_manifest_json}
        uv = UV.init("uv", data);
      </script>
      <dl>
        <dt>Dates</dt>
        <dd>{dates}</dd>
        <dt>Notes</dt>
        <dd>{notes}</dd>
        <dt>URI</dt>
        <dd>{uri}</dd>
      </dl>
      <div><a href="{iiif_manifest}"><img alt="IIIF Manifest" src="https://iiif.io/assets/uploads/logos/logo-iiif-34x30.png"></a></div>
    </main>
  </body>
</html>
