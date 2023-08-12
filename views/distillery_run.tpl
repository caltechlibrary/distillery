<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
  <title>Distillery</title>
  <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
  <style type="text/css">
    html, body, main { height: 100%; }
    body > main { padding: calc(var(--block-spacing-vertical) / 2) 0; }
    main > footer {
      position: sticky;
      top: 100vh;
    }
    h1 {
      margin-block-start: 0;
      margin-block-end: revert;
    }
    details { border-block-end: none; }
    iframe {
      margin-block-end: var(--spacing);
      max-height: 40vh;
      width: 100%;
    }
    #user [role=button] { white-space: nowrap; }
    @media (min-width: 992px) {
      main > * {
        margin-inline: 17%;
        max-width: 66%;
        padding-inline: var(--spacing);
        width: 100%;
      }
    }
  </style>
</head>

<body>
  <main class="container">
    <header>
      <h1>Distillery</h1>
    </header>
    <hr>
    <p>Processing metadata and files.</p>
    <details>
      <summary>Details</summary>
      <iframe src="{{distillery_base_url}}/run/log/{{batch_set_id}}"></iframe>
    </details>
    <div><a href="{{distillery_base_url}}">back to form</a></div>
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
            p.innerHTML = p.innerHTML.replace("Processing", "✅ Processed");
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
    <hr>
    <footer>
      <nav id="user">
        <ul>
          <li>Logged in as: {{user["display_name"]}}</li>
        </ul>
        <ul>
          <li><a href="{{distillery_base_url}}/Shibboleth.sso/Logout" role="button" class="secondary outline">Log out</a></li>
        </ul>
      </nav>
    </footer>
  </main>
</body>

</html>
