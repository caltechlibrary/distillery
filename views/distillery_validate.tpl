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
    #cancel {
      display: flex;
      margin-block-end: var(--spacing);
    }
    #cancel > a {
      align-self: center;
      margin: auto;
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
    <p>Validating metadata, files, and destinations.</p>
    <details>
      <summary>Details</summary>
      <iframe src="{{distillery_base_url}}/validate/log/{{batch_set_id}}"></iframe>
    </details>
    <form action="{{distillery_base_url}}/run" method="post">
      <input type="hidden" id="destinations" name="destinations" value="{{destinations}}">
      <input type="hidden" id="batch_set_id" name="batch_set_id" value="{{batch_set_id}}">
      <div class="grid">
        <button aria-busy="true" disabled>Validating…</button>
        <div id="cancel"><a href="{{distillery_base_url}}">Cancel</a></div>
      </div>
    </form>
    <script>
      const p = document.getElementsByTagName('p')[0];
      const iframe = document.getElementsByTagName('iframe')[0];
      const button = document.getElementsByTagName('button')[0];
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
          if (button) {
            button.innerHTML = "❌ Failure";
            button.setAttribute("aria-busy", false);
          }
        } else if (text.indexOf('🈺') != -1) {
          clearInterval(id);
          if (p) {
            p.innerHTML = p.innerHTML.replace("Validating", "✅ Successfully validated");
          }
          if (button) {
            button.innerHTML = "Run 🚀";
            button.setAttribute("aria-busy", false);
            button.disabled = false;
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
