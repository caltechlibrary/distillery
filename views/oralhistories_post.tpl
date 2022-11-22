<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
  <title>Oral Histories | Distillery</title>
  <link rel="stylesheet" href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
  <style type="text/css">
    h1 {
      margin-block: 0;
    }
    details {
      border: none;
      padding-block-start: var(--spacing);
    }
    #user [role=button] {
      white-space: nowrap;
    }
    @media (min-width: 992px) {
      main > *:not(nav, details) {
        margin-inline: 25%;
        max-width: 50%;
        padding-inline: var(--spacing);
      }
      main > nav#user, main > details {
        clear: right;
        float: right;
        width: 25%;
        padding-inline-start: var(--block-spacing-horizontal);
      }
    }
  </style>
</head>

<body>
  <main class="container">
    <nav id="user">
      <ul>
        <li>Hello, {{user["display_name"]}}!</li>
      </ul>
      <ul>
        <li><a href="{{distillery_base_url}}/Shibboleth.sso/Logout" role="button" class="secondary outline">Log out</a></li>
      </ul>
    </nav>
    <header>
      <h1>Oral Histories</h1>
      <nav>
        <ul>
          <li><a href="{{distillery_base_url}}/oralhistories">Go back to the form.</a></li>
        </ul>
      </nav>
    </header>
    <hr>
    %if error:
    <p>{{error}}</p>
    %else:
    <p>✅ the <b>{{component_id}}.docx</b> file was uploaded</p>
    <ul>
      <li>the <a href="https://github.com/{{github_repo}}/blob/main/transcripts/{{component_id}}/{{component_id}}.md">GitHub <b>{{component_id}}.md</b></a> file should be available shortly</li>
      <li>an ArchivesSpace Digital Object record should also be created for <a href="{{archivesspace_staff_url}}/search?q={{component_id}}">{{component_id}}</a></li>
      <li>any errors will be logged and sent to <i>{{user["email_address"]}}</i></li>
    </ul>
    %end
  </main>
</body>

</html>
