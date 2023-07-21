<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width">
  <title>Alchemist Regenerate | Distillery</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@next/css/pico.min.css">
  <style type="text/css">
    iframe {
      margin-block-end: var(--spacing);
      max-height: 40vh;
      width: 100%;
    }
  </style>
</head>

<body>
  <main class="container">
    <header>
      <h1>Alchemist</h1>
    </header>
    % if component_id == "_":
    <p>Regenerating metadata and files for <b>all published items</b>.</p>
    % else:
    <p>Regenerating metadata and files for <b>{{component_id}}</b>.</p>
    % end
    <details>
      <summary>Details</summary>
      <iframe src="{{distillery_base_url}}/alchemist/regenerate/log/{{component_id}}/{{timestamp}}"></iframe>
    </details>
    <div><a href="{{distillery_base_url}}/alchemist">back to form</a></div>
    <script>
      const p = document.getElementsByTagName('p')[0];
      const iframe = document.getElementsByTagName('iframe')[0];
      const details = document.getElementsByTagName('details')[0];
      let id;

      function reloadIframe() {
        iframe.contentDocument.location.reload();
        let text = iframe.contentDocument.body.innerText;
        if (text.indexOf('❌') !== -1) {
          clearInterval(id);
          if (p) {
            p.innerHTML = "❌ Something went wrong. View the details for more information.";
          }
        } else if (text.indexOf('🏁') != -1) {
          clearInterval(id);
          if (p) {
            p.innerHTML = p.innerHTML.replace("Regenerating", "✅ Regenerated");
          }
        }
        iframe.style.height = iframe.contentDocument.body.scrollHeight + 48 + 'px';
        iframe.contentWindow.scrollTo(0, iframe.contentDocument.body.scrollHeight);
      }

      function updateIframe() {
        requestAnimationFrame(reloadIframe);
      }

      details.addEventListener('toggle', updateIframe);
      id = setInterval(updateIframe, 1000);
    </script>
    % end
  </main>
</body>

</html>
