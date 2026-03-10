import figlet from 'figlet';
import {max} from 'lodash';

const READY_MSG = 'AppBase is Ready!!';

function main(): void {
  const columns = process.stdout.columns ?? 80;
  const appbase = figlet.textSync('99x Data Platform', {
    horizontalLayout: 'fitted',
    verticalLayout: 'fitted',
    whitespaceBreak: true,
    width: columns,
  });
  const border = '-'.repeat(
    max(
      appbase
        .split('\n')
        .concat(READY_MSG)
        .map((s) => s.length)
    ) ?? columns
  );
  console.log([border, appbase, border, READY_MSG, border].join('\n'));
}

if (require.main === module) {
  main();
}
