export const PILOT_ROOT='/srv/projects/sbersud/pilot/hmao/state/publication';
export function validMarker(marker,root,region){
  if(marker.region!==region)return false;
  if(marker.synthetic===true)return !/^\/(?:opt|srv)(?:\/|$)/.test(root);
  return marker.mode==='closed-pilot'&&region==='hmao'&&root===PILOT_ROOT;
}
