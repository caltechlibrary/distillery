<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width">
  <title>Alchemist \ Distillery</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@next/css/pico.min.css">
</head>

<body>
  <main class="container">
    <header>
      <h1>Alchemist</h1>
    </header>
    <form action="{{distillery_base_url}}/alchemist/regenerate" method="post" enctype="multipart/form-data">
      <fieldset>
        <legend>Regenerate Web Files</legend>
        <p>Rebuild the <code>index.html</code> and <code>manifest.json</code> files to capture metadata updates and/or template changes.</p>
        <label><input type="radio" name="regenerate" value="one">Regenerate files for one item</label>
        <label>Component Unique Identifier<input type="text" name="component_id"></label>
        <label><input type="radio" name="regenerate" value="all">Regenerate files for all items</label>
      </fieldset>
      <input type="submit" name="regenerate" value="Regenerate">
    </form>
  </main>
  <script>
    const component_id = document.querySelector('input[name="component_id"]');
    component_id.parentElement.hidden = true;
    function handleRadioChoice() {
      if (document.querySelector('input[value="one"]').checked) {
        component_id.parentElement.hidden = false;
        component_id.required = true;
      }
      else {
        component_id.parentElement.hidden = true;
        component_id.required = false;
        component_id.value = '';
      }
    }
    const radios = document.querySelectorAll('input[name="regenerate"]');
    radios.forEach(radio => radio.addEventListener('change', handleRadioChoice));
  </script>
</body>

</html>
